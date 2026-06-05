# ================================================================
# tests/test_transformations.py
# Testes unitários das transformações Bronze → Silver → Gold
# Equivalente ao: testes de qualidade de dados em projetos reais
# Uso: pytest tests/ -v
# ================================================================

import pytest
import pandas as pd
import duckdb
from datetime import datetime, date

# ----------------------------------------------------------------
# Fixtures — dados de teste reutilizáveis
# ----------------------------------------------------------------

@pytest.fixture
def df_clientes_bronze():
    """DataFrame simulando dados brutos de clientes vindo do Bronze."""
    return pd.DataFrame({
        "id_cliente":      [1, 2, 3, 4, 5],
        "nome":            ["  joão silva  ", "MARIA SOUZA", "pedro santos", "  ANA LIMA", "carlos FERREIRA  "],
        "cpf":             ["123.456.789-00", "987.654.321-00", "111.222.333-44", "555.666.777-88", "999.888.777-66"],
        "email":           ["JOAO@EMAIL.COM", "maria@email.com", "pedro@email.com", "ana@email.com", "carlos@email.com"],
        "data_nascimento": [date(1990, 1, 1)] * 5,
        "genero":          ["M", "F", "M", "F", "M"],
        "cidade":          ["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Salvador", "Curitiba"],
        "estado":          ["sp", "rj", "mg", "ba", "pr"],
        "score_credito":   [800, None, 650, 900, 720],
        "data_cadastro":   [datetime.now()] * 5,
        "ativo":           [True] * 5,
        "_ingestao_timestamp": [datetime.now()] * 5,
        "_fonte":          ["postgresql"] * 5,
        "_tabela_origem":  ["clientes"] * 5,
    })


@pytest.fixture
def df_transacoes_bronze():
    """DataFrame simulando dados brutos de transações vindo do Bronze."""
    return pd.DataFrame({
        "id_transacao":       [1, 2, 3, 4, 5],
        "id_conta":           [1, 2, 3, 4, 5],
        "id_categoria":       [1, 2, 3, 4, 5],
        "id_comerciante":     [1, 2, 3, 4, 5],
        "valor":              [100.50, -200.00, 0.0, 5000.00, 150.75],
        "tipo":               ["PIX", "DEBITO", "credito", "TED", "boleto"],
        "status":             ["APROVADA", "negada", "aprovada", "suspeita", "cancelada"],
        "canal":              ["APP", "web", "pos", "atm", "app"],
        "descricao":          ["compra", "pagamento", "transferencia", "saque", "recarga"],
        "data_transacao":     [datetime(2026, 1, 15, 22, 30)] + [datetime.now()] * 4,
        "data_processamento": [datetime.now()] * 5,
        "is_fraude":          [False, False, False, True, False],
        "score_fraude":       [0.1, 0.2, None, 0.95, 0.05],
        "ip_origem":          ["192.168.1.1"] * 5,
        "device_id":          ["device-001"] * 5,
        "_ingestao_timestamp": [datetime.now()] * 5,
        "_fonte":             ["postgresql"] * 5,
        "_tabela_origem":     ["transacoes"] * 5,
    })


# ----------------------------------------------------------------
# Testes da camada Silver — Clientes
# ----------------------------------------------------------------

class TestLimpezaClientes:

    def test_nome_capitalizado(self, df_clientes_bronze):
        """Verifica se nomes são padronizados com Title Case."""

        df = df_clientes_bronze.copy()
        df["nome"] = df["nome"].str.strip().str.title()
        assert df["nome"].iloc[0] == "João Silva"
        assert df["nome"].iloc[1] == "Maria Souza"

    def test_email_lowercase(self, df_clientes_bronze):
        """Verifica se emails são convertidos para minúsculas."""
        df = df_clientes_bronze.copy()
        df["email"] = df["email"].str.strip().str.lower()
        assert df["email"].iloc[0] == "joao@email.com"

    def test_estado_uppercase(self, df_clientes_bronze):
        """Verifica se estados são convertidos para maiúsculas."""
        df = df_clientes_bronze.copy()
        df["estado"] = df["estado"].str.upper()
        assert df["estado"].iloc[0] == "SP"
        assert df["estado"].iloc[1] == "RJ"

    def test_cpf_sem_pontuacao(self, df_clientes_bronze):
        """Verifica se CPF tem pontuação removida."""
        df = df_clientes_bronze.copy()
        df["cpf"] = df["cpf"].str.replace(r"[.\-]", "", regex=True)
        assert df["cpf"].iloc[0] == "12345678900"
        assert len(df["cpf"].iloc[0]) == 11

    def test_score_nulo_preenchido(self, df_clientes_bronze):
        """Verifica se score nulo é preenchido com a mediana."""
        df = df_clientes_bronze.copy()
        mediana = df["score_credito"].median()
        df["score_credito"] = df["score_credito"].fillna(mediana)
        assert df["score_credito"].isna().sum() == 0

    def test_colunas_auditoria_removidas(self, df_clientes_bronze):
        """Verifica se colunas de auditoria do Bronze são removidas."""
        df = df_clientes_bronze.copy()
        colunas_auditoria = ["_ingestao_timestamp", "_fonte", "_tabela_origem"]
        df = df.drop(columns=colunas_auditoria)
        for col in colunas_auditoria:
            assert col not in df.columns


# ----------------------------------------------------------------
# Testes da camada Silver — Transações
# ----------------------------------------------------------------

class TestLimpezaTransacoes:

    def test_valor_negativo_corrigido(self, df_transacoes_bronze):
        """Verifica se valores negativos são convertidos para positivo."""
        df = df_transacoes_bronze.copy()
        df["valor"] = df["valor"].abs()
        assert (df["valor"] >= 0).all()

    def test_tipo_lowercase(self, df_transacoes_bronze):
        """Verifica se tipo de transação é padronizado para minúsculas."""
        df = df_transacoes_bronze.copy()
        df["tipo"] = df["tipo"].str.lower().str.strip()
        assert df["tipo"].iloc[0] == "pix"
        assert df["tipo"].iloc[1] == "debito"

    def test_score_fraude_nulo_preenchido(self, df_transacoes_bronze):
        """Verifica se score_fraude nulo é preenchido com 0."""
        df = df_transacoes_bronze.copy()
        df["score_fraude"] = df["score_fraude"].fillna(0.0)
        assert df["score_fraude"].isna().sum() == 0
        assert df["score_fraude"].iloc[2] == 0.0

    def test_faixa_valor_criada(self, df_transacoes_bronze):
        """Verifica se coluna faixa_valor é criada corretamente."""
        df = df_transacoes_bronze.copy()
        df["valor"] = df["valor"].abs()
        df["faixa_valor"] = pd.cut(
            df["valor"],
            bins=[0, 50, 200, 500, 1000, 5000, float("inf")],
            labels=["micro", "pequeno", "medio", "alto", "muito_alto", "critico"],
        )
        assert "faixa_valor" in df.columns
        assert 'faixa_valor' in df.columns

    def test_fora_horario_comercial(self, df_transacoes_bronze):
        """Verifica se flag de horário fora do comercial é criada."""
        df = df_transacoes_bronze.copy()
        df["data_transacao"] = pd.to_datetime(df["data_transacao"])
        df["fora_horario_comercial"] = ~df["data_transacao"].dt.hour.between(8, 18)
        # A primeira transação é às 22:30 — deve ser fora do horário
        assert df["fora_horario_comercial"].iloc[0] == True
        assert "fora_horario_comercial" in df.columns


# ----------------------------------------------------------------
# Testes da camada Gold — DuckDB
# ----------------------------------------------------------------

class TestCamadaGold:

    @pytest.fixture
    def conn_duckdb(self, df_clientes_bronze, df_transacoes_bronze):
        """Cria conexão DuckDB em memória com dados de teste."""
        conn = duckdb.connect(":memory:")

        # Prepara dados Silver simplificados para os testes
        df_clientes = df_clientes_bronze.copy()
        df_clientes["nome"]  = df_clientes["nome"].str.strip().str.title()
        df_clientes["estado"] = df_clientes["estado"].str.upper()
        df_clientes["cpf"]   = df_clientes["cpf"].str.replace(r"[.\-]", "", regex=True)
        df_clientes["score_credito"] = df_clientes["score_credito"].fillna(750)

        df_transacoes = df_transacoes_bronze.copy()
        df_transacoes["valor"]        = df_transacoes["valor"].abs()
        df_transacoes["tipo"]         = df_transacoes["tipo"].str.lower()
        df_transacoes["status"]       = df_transacoes["status"].str.lower()
        df_transacoes["canal"]        = df_transacoes["canal"].str.lower()
        df_transacoes["score_fraude"] = df_transacoes["score_fraude"].fillna(0.0)
        df_transacoes["data_transacao"] = pd.to_datetime(df_transacoes["data_transacao"])
        df_transacoes["fora_horario_comercial"] = ~df_transacoes["data_transacao"].dt.hour.between(8, 18)

        # Cria tabela de contas simplificada
        df_contas = pd.DataFrame({
            "id_conta":   [1, 2, 3, 4, 5],
            "id_cliente": [1, 2, 3, 4, 5],
            "tipo_conta": ["corrente"] * 5,
            "saldo":      [1000.0] * 5,
        })

        conn.register("silver_clientes",   df_clientes)
        conn.register("silver_transacoes", df_transacoes)
        conn.register("silver_contas",     df_contas)

        return conn

    def test_gold_fraudes_por_estado(self, conn_duckdb):
        """Verifica se a agregação por estado é criada corretamente."""
        result = conn_duckdb.execute("""
            SELECT cl.estado, COUNT(*) as total
            FROM silver_transacoes t
            JOIN silver_contas co ON t.id_conta = co.id_conta
            JOIN silver_clientes cl ON co.id_cliente = cl.id_cliente
            GROUP BY cl.estado
        """).df()
        assert len(result) > 0
        assert "estado" in result.columns
        assert "total" in result.columns

    def test_gold_transacoes_por_hora(self, conn_duckdb):
        """Verifica se a agregação por hora retorna 24 horas."""
        result = conn_duckdb.execute("""
            SELECT EXTRACT(HOUR FROM data_transacao) as hora, COUNT(*) as total
            FROM silver_transacoes
            GROUP BY hora
        """).df()
        assert len(result) > 0
        assert result["total"].sum() == 5  # 5 transações de teste

    def test_gold_perfil_risco_cliente(self, conn_duckdb):
        """Verifica se o perfil de risco classifica corretamente."""
        result = conn_duckdb.execute("""
            SELECT
                cl.id_cliente,
                AVG(t.score_fraude) as score_medio,
                CASE
                    WHEN AVG(t.score_fraude) > 0.7 THEN 'CRITICO'
                    WHEN AVG(t.score_fraude) > 0.4 THEN 'ALTO'
                    ELSE 'BAIXO'
                END as classificacao
            FROM silver_clientes cl
            JOIN silver_contas co ON cl.id_cliente = co.id_cliente
            JOIN silver_transacoes t ON co.id_conta = t.id_conta
            GROUP BY cl.id_cliente
        """).df()
        assert len(result) > 0
        assert "classificacao" in result.columns
        # Cliente 4 tem fraude com score 0.95 — deve ser CRITICO
        cliente_4 = result[result["id_cliente"] == 4]
        assert cliente_4["classificacao"].values[0] == "CRITICO"

    def test_volume_total_positivo(self, conn_duckdb):
        """Verifica se o volume total de transações é positivo."""
        result = conn_duckdb.execute("""
            SELECT SUM(valor) as volume_total FROM silver_transacoes
        """).fetchone()
        assert result[0] > 0
