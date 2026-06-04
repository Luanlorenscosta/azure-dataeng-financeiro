# ================================================================
# generate_fake_data.py
# Gera dados financeiros sintéticos e popula o PostgreSQL
# Uso: python sql/seed_data/generate_fake_data.py
# ================================================================

import random
import psycopg2
from faker import Faker
from datetime import datetime, timedelta
from loguru import logger

# ----------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------
fake = Faker("pt_BR")
random.seed(42)

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "financeiro_source",
    "user":     "postgres",
    "password": "postgres123",
}

QTD_CLIENTES    = 500
QTD_COMERCIANTES = 100
QTD_TRANSACOES  = 50_000

ESTADOS_BR = ["SP","RJ","MG","BA","RS","PR","PE","CE","GO","SC"]

TIPOS_TRANSACAO = ["debito","credito","pix","ted","doc","boleto"]
STATUS_TRANSACAO = ["aprovada","aprovada","aprovada","aprovada","negada","cancelada","pendente","suspeita"]
CANAIS = ["app","app","app","web","pos","atm","agencia"]

# ----------------------------------------------------------------
# Conexão
# ----------------------------------------------------------------
def conectar():
    logger.info("Conectando ao PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    logger.success("Conectado!")
    return conn

# ----------------------------------------------------------------
# Geração de clientes
# ----------------------------------------------------------------
def gerar_clientes(cursor, qtd):
    logger.info(f"Gerando {qtd} clientes...")
    cpfs_usados = set()
    emails_usados = set()
    inseridos = 0

    for _ in range(qtd):
        cpf = fake.cpf()
        email = fake.email()
        if cpf in cpfs_usados or email in emails_usados:
            continue
        cpfs_usados.add(cpf)
        emails_usados.add(email)

        cursor.execute("""
            INSERT INTO clientes
                (nome, cpf, email, data_nascimento, genero, cidade, estado, score_credito)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            fake.name(),
            cpf,
            email,
            fake.date_of_birth(minimum_age=18, maximum_age=75),
            random.choice(["M","F","O"]),
            fake.city(),
            random.choice(ESTADOS_BR),
            random.randint(300, 1000),
        ))
        inseridos += 1

    logger.success(f"{inseridos} clientes inseridos.")
    return inseridos

# ----------------------------------------------------------------
# Geração de contas
# ----------------------------------------------------------------
def gerar_contas(cursor, qtd_clientes):
    logger.info("Gerando contas bancárias...")
    cursor.execute("SELECT id_cliente FROM clientes")
    clientes = [r[0] for r in cursor.fetchall()]
    numeros_usados = set()
    inseridos = 0

    for id_cliente in clientes:
        # Cada cliente tem entre 1 e 3 contas
        for _ in range(random.randint(1, 3)):
            numero = fake.numerify("####-#####-#")
            if numero in numeros_usados:
                continue
            numeros_usados.add(numero)

            tipo = random.choice(["corrente","poupanca","investimento","cartao"])
            saldo = round(random.uniform(-500, 50000), 2)
            limite = round(random.uniform(1000, 20000), 2) if tipo == "cartao" else 0

            cursor.execute("""
                INSERT INTO contas (id_cliente, numero_conta, tipo_conta, saldo, limite)
                VALUES (%s, %s, %s, %s, %s)
            """, (id_cliente, numero, tipo, saldo, limite))
            inseridos += 1

    logger.success(f"{inseridos} contas inseridas.")
    return inseridos

# ----------------------------------------------------------------
# Geração de comerciantes
# ----------------------------------------------------------------
def gerar_comerciantes(cursor, qtd):
    logger.info(f"Gerando {qtd} comerciantes...")
    categorias = [
        "alimentacao","transporte","saude","educacao",
        "lazer","compras","servicos","outros"
    ]
    cnpjs_usados = set()

    for _ in range(qtd):
        cnpj = fake.cnpj()
        if cnpj in cnpjs_usados:
            continue
        cnpjs_usados.add(cnpj)

        cursor.execute("""
            INSERT INTO comerciantes (nome, categoria, cidade, estado, cnpj)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            fake.company(),
            random.choice(categorias),
            fake.city(),
            random.choice(ESTADOS_BR),
            cnpj,
        ))

    logger.success(f"{qtd} comerciantes inseridos.")

# ----------------------------------------------------------------
# Geração de transações
# ----------------------------------------------------------------
def gerar_transacoes(cursor, qtd):
    logger.info(f"Gerando {qtd} transações...")

    cursor.execute("SELECT id_conta FROM contas")
    contas = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT id_categoria FROM categorias_transacao")
    categorias = [r[0] for r in cursor.fetchall()]

    cursor.execute("SELECT id_comerciante FROM comerciantes")
    comerciantes = [r[0] for r in cursor.fetchall()]

    data_inicio = datetime.now() - timedelta(days=365)
    batch = []
    BATCH_SIZE = 1000

    for i in range(qtd):
        data_transacao = data_inicio + timedelta(
            seconds=random.randint(0, 365 * 24 * 3600)
        )

        # Lógica de fraude — 2% das transações são suspeitas
        is_fraude = random.random() < 0.02
        score_fraude = round(random.uniform(0.7, 1.0), 4) if is_fraude else round(random.uniform(0.0, 0.3), 4)
        status = "suspeita" if is_fraude else random.choice(STATUS_TRANSACAO)

        # Valores maiores para fraudes
        valor = round(random.uniform(500, 15000), 2) if is_fraude else round(random.uniform(1, 3000), 2)

        batch.append((
            random.choice(contas),
            random.choice(categorias),
            random.choice(comerciantes) if random.random() > 0.2 else None,
            valor,
            random.choice(TIPOS_TRANSACAO),
            status,
            random.choice(CANAIS),
            fake.sentence(nb_words=4),
            data_transacao,
            data_transacao + timedelta(seconds=random.randint(1, 300)),
            is_fraude,
            score_fraude,
            fake.ipv4(),
            fake.uuid4(),
        ))

        if len(batch) >= BATCH_SIZE:
            cursor.executemany("""
                INSERT INTO transacoes
                    (id_conta, id_categoria, id_comerciante, valor, tipo,
                     status, canal, descricao, data_transacao, data_processamento,
                     is_fraude, score_fraude, ip_origem, device_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, batch)
            batch = []
            logger.info(f"  {i+1}/{qtd} transações inseridas...")

    if batch:
        cursor.executemany("""
            INSERT INTO transacoes
                (id_conta, id_categoria, id_comerciante, valor, tipo,
                 status, canal, descricao, data_transacao, data_processamento,
                 is_fraude, score_fraude, ip_origem, device_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, batch)

    logger.success(f"{qtd} transações inseridas.")

# ----------------------------------------------------------------
# Main
# ----------------------------------------------------------------
def main():
    logger.info("=" * 50)
    logger.info("GERADOR DE DADOS FINANCEIROS SINTÉTICOS")
    logger.info("=" * 50)

    conn = conectar()
    cursor = conn.cursor()

    try:
        gerar_clientes(cursor, QTD_CLIENTES)
        conn.commit()

        gerar_contas(cursor, QTD_CLIENTES)
        conn.commit()

        gerar_comerciantes(cursor, QTD_COMERCIANTES)
        conn.commit()

        gerar_transacoes(cursor, QTD_TRANSACOES)
        conn.commit()

        # Resumo final
        cursor.execute("SELECT COUNT(*) FROM clientes")
        logger.success(f"Clientes:     {cursor.fetchone()[0]:>8,}")
        cursor.execute("SELECT COUNT(*) FROM contas")
        logger.success(f"Contas:       {cursor.fetchone()[0]:>8,}")
        cursor.execute("SELECT COUNT(*) FROM comerciantes")
        logger.success(f"Comerciantes: {cursor.fetchone()[0]:>8,}")
        cursor.execute("SELECT COUNT(*) FROM transacoes")
        logger.success(f"Transações:   {cursor.fetchone()[0]:>8,}")
        cursor.execute("SELECT COUNT(*) FROM transacoes WHERE is_fraude = TRUE")
        logger.success(f"Fraudes:      {cursor.fetchone()[0]:>8,}")

        logger.success("Dados gerados com sucesso!")

    except Exception as e:
        conn.rollback()
        logger.error(f"Erro: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
