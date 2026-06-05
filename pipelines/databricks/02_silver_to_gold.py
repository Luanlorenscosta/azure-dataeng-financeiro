# ================================================================
# 02_silver_to_gold.py
# Transformação: Silver → Gold
# Equivalente ao: Synapse Analytics / Databricks Gold Layer
# O que faz:
#   - Lê os Parquet da camada Silver
#   - Cria agregações e indicadores de negócio
#   - Grava na camada Gold (DuckDB + Parquet)
#   - Simula um Data Warehouse analítico
# Uso: python pipelines/databricks/02_silver_to_gold.py
# ================================================================

import pandas as pd
import duckdb
import json
from pathlib import Path
from datetime import datetime
from loguru import logger

# ----------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------
DATA_LAKE = Path("data_lake")
SILVER    = DATA_LAKE / "silver"
GOLD      = DATA_LAKE / "gold"

# DuckDB — banco analítico local (equivalente ao Synapse SQL Pool)
# O arquivo .duckdb persiste os dados entre execuções
DB_PATH   = DATA_LAKE / "financeiro_dw.duckdb"

# ----------------------------------------------------------------
# Leitura da camada Silver
# ----------------------------------------------------------------
def ler_silver(tabela: str) -> pd.DataFrame:
    """Lê todos os Parquet de uma tabela da camada Silver."""
    arquivos = list((SILVER / tabela).rglob("*.parquet"))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em silver/{tabela}")
    dfs = [pd.read_parquet(f) for f in arquivos]
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Silver {tabela}: {len(df):,} linhas")
    return df

# ----------------------------------------------------------------
# Agregações Gold — cada função cria UMA tabela analítica
# ----------------------------------------------------------------

def gold_resumo_fraudes_por_estado(conn: duckdb.DuckDBPyConnection):
    """
    Gold 1: Resumo de fraudes por estado.
    Responde: Onde estão concentradas as fraudes no Brasil?
    Útil para: times de risco, compliance, mapa de calor no Power BI.
    """
    logger.info("Criando: gold_resumo_fraudes_por_estado")

    conn.execute("""
        CREATE OR REPLACE TABLE gold_resumo_fraudes_por_estado AS
        SELECT
            cl.estado,
            COUNT(t.id_transacao)                                    AS total_transacoes,
            SUM(CASE WHEN t.is_fraude THEN 1 ELSE 0 END)            AS total_fraudes,
            ROUND(
                SUM(CASE WHEN t.is_fraude THEN 1 ELSE 0 END) * 100.0
                / COUNT(t.id_transacao), 2
            )                                                        AS pct_fraude,
            ROUND(SUM(t.valor), 2)                                   AS valor_total,
            ROUND(SUM(CASE WHEN t.is_fraude THEN t.valor ELSE 0 END), 2)
                                                                     AS valor_fraudes,
            ROUND(AVG(t.score_fraude), 4)                            AS score_fraude_medio,
            ROUND(AVG(t.valor), 2)                                   AS ticket_medio
        FROM silver_transacoes t
        JOIN silver_contas     co ON t.id_conta   = co.id_conta
        JOIN silver_clientes   cl ON co.id_cliente = cl.id_cliente
        GROUP BY cl.estado
        ORDER BY pct_fraude DESC
    """)

    total = conn.execute("SELECT COUNT(*) FROM gold_resumo_fraudes_por_estado").fetchone()[0]
    logger.success(f"  ✓ gold_resumo_fraudes_por_estado: {total} estados")


def gold_transacoes_por_hora(conn: duckdb.DuckDBPyConnection):
    """
    Gold 2: Volume de transações por hora do dia.
    Responde: Quando o sistema está mais sobrecarregado?
              Horários de maior risco de fraude?
    Útil para: dimensionamento de infraestrutura, alertas de fraude.
    """
    logger.info("Criando: gold_transacoes_por_hora")

    conn.execute("""
        CREATE OR REPLACE TABLE gold_transacoes_por_hora AS
        SELECT
            EXTRACT(HOUR FROM data_transacao)           AS hora_dia,
            COUNT(*)                                    AS total_transacoes,
            SUM(CASE WHEN is_fraude THEN 1 ELSE 0 END) AS total_fraudes,
            ROUND(AVG(valor), 2)                        AS ticket_medio,
            ROUND(SUM(valor), 2)                        AS volume_total,
            ROUND(
                SUM(CASE WHEN is_fraude THEN 1 ELSE 0 END) * 100.0
                / COUNT(*), 2
            )                                           AS pct_fraude,
            COUNT(DISTINCT id_conta)                    AS contas_ativas
        FROM silver_transacoes
        GROUP BY hora_dia
        ORDER BY hora_dia
    """)

    total = conn.execute("SELECT COUNT(*) FROM gold_transacoes_por_hora").fetchone()[0]
    logger.success(f"  ✓ gold_transacoes_por_hora: {total} horas mapeadas")


def gold_perfil_risco_cliente(conn: duckdb.DuckDBPyConnection):
    """
    Gold 3: Perfil de risco por cliente.
    Responde: Quais clientes merecem atenção especial do time de risco?
    Útil para: modelos de ML, prevenção a fraude, limit de crédito.
    """
    logger.info("Criando: gold_perfil_risco_cliente")

    conn.execute("""
        CREATE OR REPLACE TABLE gold_perfil_risco_cliente AS
        SELECT
            cl.id_cliente,
            cl.nome,
            cl.estado,
            cl.score_credito,
            COUNT(t.id_transacao)                                     AS total_transacoes,
            SUM(CASE WHEN t.is_fraude THEN 1 ELSE 0 END)             AS total_fraudes,
            ROUND(SUM(t.valor), 2)                                    AS volume_total,
            ROUND(AVG(t.valor), 2)                                    AS ticket_medio,
            ROUND(MAX(t.valor), 2)                                    AS maior_transacao,
            ROUND(AVG(t.score_fraude), 4)                             AS score_fraude_medio,
            COUNT(DISTINCT co.id_conta)                               AS qtd_contas,
            SUM(CASE WHEN t.fora_horario_comercial THEN 1 ELSE 0 END) AS transacoes_fora_horario,
            -- Score de risco composto (0 a 100)
            ROUND(
                (AVG(t.score_fraude) * 40)
                + (SUM(CASE WHEN t.is_fraude THEN 1 ELSE 0 END) * 5)
                + (SUM(CASE WHEN t.fora_horario_comercial THEN 1 ELSE 0 END) * 0.5)
                , 2
            )                                                          AS score_risco_composto,
            -- Classificação de risco
            CASE
                WHEN AVG(t.score_fraude) > 0.7 THEN 'CRITICO'
                WHEN AVG(t.score_fraude) > 0.4 THEN 'ALTO'
                WHEN AVG(t.score_fraude) > 0.2 THEN 'MEDIO'
                ELSE 'BAIXO'
            END                                                        AS classificacao_risco
        FROM silver_clientes   cl
        JOIN silver_contas     co ON cl.id_cliente = co.id_cliente
        JOIN silver_transacoes t  ON co.id_conta   = t.id_conta
        GROUP BY cl.id_cliente, cl.nome, cl.estado, cl.score_credito
        ORDER BY score_risco_composto DESC
    """)

    total = conn.execute("SELECT COUNT(*) FROM gold_perfil_risco_cliente").fetchone()[0]
    criticos = conn.execute(
        "SELECT COUNT(*) FROM gold_perfil_risco_cliente WHERE classificacao_risco = 'CRITICO'"
    ).fetchone()[0]
    logger.success(f"  ✓ gold_perfil_risco_cliente: {total} clientes | {criticos} críticos")


def gold_resumo_executivo_diario(conn: duckdb.DuckDBPyConnection):
    """
    Gold 4: KPIs diários do negócio.
    Responde: Como foi o dia? (pergunta que todo C-level faz)
    Útil para: dashboard executivo, relatórios automáticos.
    """
    logger.info("Criando: gold_resumo_executivo_diario")

    conn.execute("""
        CREATE OR REPLACE TABLE gold_resumo_executivo_diario AS
        SELECT
            CAST(data_transacao AS DATE)                              AS data,
            COUNT(*)                                                  AS total_transacoes,
            COUNT(DISTINCT id_conta)                                  AS contas_ativas,
            ROUND(SUM(valor), 2)                                      AS volume_total,
            ROUND(AVG(valor), 2)                                      AS ticket_medio,
            SUM(CASE WHEN is_fraude     THEN 1 ELSE 0 END)           AS total_fraudes,
            SUM(CASE WHEN status = 'aprovada'  THEN 1 ELSE 0 END)    AS aprovadas,
            SUM(CASE WHEN status = 'negada'    THEN 1 ELSE 0 END)    AS negadas,
            SUM(CASE WHEN status = 'cancelada' THEN 1 ELSE 0 END)    AS canceladas,
            SUM(CASE WHEN status = 'suspeita'  THEN 1 ELSE 0 END)    AS suspeitas,
            SUM(CASE WHEN canal = 'app'        THEN 1 ELSE 0 END)    AS via_app,
            SUM(CASE WHEN canal = 'web'        THEN 1 ELSE 0 END)    AS via_web,
            SUM(CASE WHEN canal = 'pos'        THEN 1 ELSE 0 END)    AS via_pos,
            ROUND(
                SUM(CASE WHEN is_fraude THEN 1 ELSE 0 END) * 100.0
                / COUNT(*), 4
            )                                                         AS taxa_fraude_pct
        FROM silver_transacoes
        GROUP BY CAST(data_transacao AS DATE)
        ORDER BY data
    """)

    total = conn.execute("SELECT COUNT(*) FROM gold_resumo_executivo_diario").fetchone()[0]
    logger.success(f"  ✓ gold_resumo_executivo_diario: {total} dias de dados")


def exportar_gold_para_parquet(conn: duckdb.DuckDBPyConnection):
    """
    Exporta todas as tabelas Gold para Parquet.
    No Azure: essas tabelas ficariam no ADLS Gen2 camada Gold
    e seriam lidas pelo Power BI via DirectQuery no Synapse.
    """
    logger.info("Exportando tabelas Gold para Parquet...")

    tabelas_gold = [
        "gold_resumo_fraudes_por_estado",
        "gold_transacoes_por_hora",
        "gold_perfil_risco_cliente",
        "gold_resumo_executivo_diario",
    ]

    agora = datetime.now()
    particao = f"ano={agora.year}/mes={agora.month:02d}/dia={agora.day:02d}"

    for tabela in tabelas_gold:
        destino = GOLD / tabela / particao
        destino.mkdir(parents=True, exist_ok=True)
        arquivo = destino / f"{tabela}.parquet"

        df = conn.execute(f"SELECT * FROM {tabela}").df()
        df["_gold_timestamp"] = datetime.now()
        df.to_parquet(arquivo, index=False)

        tamanho_kb = arquivo.stat().st_size / 1024
        logger.success(f"  ✓ {tabela}: {len(df)} linhas | {tamanho_kb:.1f} KB → {arquivo}")


def imprimir_insights(conn: duckdb.DuckDBPyConnection):
    """Imprime os principais insights dos dados — demonstra visão analítica."""

    logger.info("\n" + "=" * 55)
    logger.info("INSIGHTS DO NEGÓCIO")
    logger.info("=" * 55)

    # Top 3 estados com mais fraude
    logger.info("\n🔴 Top 3 estados com maior taxa de fraude:")
    rows = conn.execute("""
        SELECT estado, pct_fraude, total_fraudes, valor_fraudes
        FROM gold_resumo_fraudes_por_estado
        ORDER BY pct_fraude DESC LIMIT 3
    """).fetchall()
    for r in rows:
        logger.info(f"   {r[0]}: {r[1]}% fraude | {r[2]} ocorrências | R$ {r[3]:,.2f}")

    # Hora de maior risco
    logger.info("\n⏰ Hora do dia com maior taxa de fraude:")
    row = conn.execute("""
        SELECT hora_dia, pct_fraude, total_transacoes
        FROM gold_transacoes_por_hora
        ORDER BY pct_fraude DESC LIMIT 1
    """).fetchone()
    logger.info(f"   {int(row[0])}h: {row[1]}% fraude em {row[2]:,} transações")

    # Clientes críticos
    logger.info("\n⚠️  Clientes em nível CRÍTICO de risco:")
    row = conn.execute("""
        SELECT COUNT(*), ROUND(AVG(score_risco_composto),2)
        FROM gold_perfil_risco_cliente
        WHERE classificacao_risco = 'CRITICO'
    """).fetchone()
    logger.info(f"   {row[0]} clientes críticos | Score médio: {row[1]}")

    # KPI do melhor dia
    logger.info("\n📈 Melhor dia em volume:")
    row = conn.execute("""
        SELECT data, volume_total, total_transacoes, taxa_fraude_pct
        FROM gold_resumo_executivo_diario
        ORDER BY volume_total DESC LIMIT 1
    """).fetchone()
    logger.info(f"   {row[0]}: R$ {row[1]:,.2f} | {row[2]:,} transações | {row[3]}% fraude")


# ----------------------------------------------------------------
# Main
# ----------------------------------------------------------------
def main():
    logger.info("=" * 55)
    logger.info("PIPELINE SILVER → GOLD")
    logger.info("Equivalente: Synapse Analytics / Databricks Gold")
    logger.info("=" * 55)

    # Conecta ao DuckDB (cria o arquivo se não existir)
    logger.info(f"Conectando ao DuckDB: {DB_PATH}")
    DATA_LAKE.mkdir(exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))

    # Carrega tabelas Silver como views no DuckDB
    # Isso é equivalente a registrar DataFrames no Spark
    logger.info("Registrando tabelas Silver no DuckDB...")
    tabelas = ["clientes", "contas", "transacoes", "comerciantes", "categorias_transacao"]

    for tabela in tabelas:
        df = ler_silver(tabela)
        conn.register(f"silver_{tabela}", df)

    # Cria as tabelas Gold
    logger.info("\nCriando agregações Gold...")
    gold_resumo_fraudes_por_estado(conn)
    gold_transacoes_por_hora(conn)
    gold_perfil_risco_cliente(conn)
    gold_resumo_executivo_diario(conn)

    # Exporta para Parquet
    exportar_gold_para_parquet(conn)

    # Insights
    imprimir_insights(conn)

    logger.info("\n" + "=" * 55)
    logger.success("Pipeline Gold concluído!")
    logger.info(f"DuckDB salvo em: {DB_PATH}")
    logger.info("=" * 55)

    conn.close()

if __name__ == "__main__":
    main()
