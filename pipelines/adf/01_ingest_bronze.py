# ================================================================
# 01_ingest_bronze.py
# Pipeline de ingestão: PostgreSQL → Bronze (Parquet)
# Equivalente ao: Azure Data Factory pipeline
# Uso: python pipelines/adf/01_ingest_bronze.py
# ================================================================

import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from pathlib import Path
from loguru import logger
from sqlalchemy import create_engine, text

# ----------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------
DB_URL = "postgresql://postgres:postgres123@localhost:5433/financeiro_source"

# Estrutura do Data Lake local (simula ADLS Gen2)
DATA_LAKE_PATH = Path("data_lake")
BRONZE_PATH    = DATA_LAKE_PATH / "bronze"

# Tabelas para ingerir
TABELAS = [
    "clientes",
    "contas",
    "comerciantes",
    "categorias_transacao",
    "transacoes",
]

# ----------------------------------------------------------------
# Funções auxiliares
# ----------------------------------------------------------------
def criar_estrutura_data_lake():
    """Cria as pastas Bronze/Silver/Gold localmente."""
    for camada in ["bronze", "silver", "gold"]:
        for tabela in TABELAS:
            pasta = DATA_LAKE_PATH / camada / tabela
            pasta.mkdir(parents=True, exist_ok=True)
    logger.info("Estrutura do Data Lake criada.")

def gerar_particao():
    """Retorna a partição no formato ano=YYYY/mes=MM/dia=DD."""
    agora = datetime.now()
    return f"ano={agora.year}/mes={agora.month:02d}/dia={agora.day:02d}"

def ingerir_tabela(engine, tabela: str, particao: str):
    """
    Lê uma tabela do PostgreSQL e grava em Parquet na camada Bronze.
    Simula o comportamento de um pipeline do ADF com:
    - Source: PostgreSQL (Linked Service)
    - Sink: ADLS Gen2 Bronze container (Dataset Parquet)
    - Particionamento por data de carga
    """
    logger.info(f"Ingerindo tabela: {tabela}")

    try:
        # Lê a tabela completa (full load)
        # Em produção no ADF usaríamos watermark para carga incremental
        df = pd.read_sql(f"SELECT * FROM {tabela}", engine)

        # Adiciona metadados de auditoria (padrão em pipelines de dados)
        df["_ingestao_timestamp"] = datetime.now()
        df["_fonte"]              = "postgresql://financeiro_source"
        df["_tabela_origem"]      = tabela

        # Caminho de destino particionado
        destino = BRONZE_PATH / tabela / particao
        destino.mkdir(parents=True, exist_ok=True)

        arquivo = destino / f"{tabela}.parquet"

        # Grava em Parquet (formato padrão do Data Lake)
        df.to_parquet(arquivo, index=False, engine="pyarrow")

        # Estatísticas
        tamanho_mb = arquivo.stat().st_size / (1024 * 1024)
        logger.success(
            f"  ✓ {tabela}: {len(df):,} linhas | "
            f"{df.shape[1]} colunas | "
            f"{tamanho_mb:.2f} MB → {arquivo}"
        )

        return {
            "tabela":    tabela,
            "linhas":    len(df),
            "colunas":   df.shape[1],
            "tamanho_mb": round(tamanho_mb, 2),
            "arquivo":   str(arquivo),
            "status":    "sucesso",
        }

    except Exception as e:
        logger.error(f"  ✗ Erro ao ingerir {tabela}: {e}")
        return {
            "tabela": tabela,
            "status": "erro",
            "erro":   str(e),
        }

def gerar_manifesto(resultados: list, particao: str):
    """
    Grava um manifesto JSON com metadados da carga.
    Equivalente ao log de execução do ADF pipeline run.
    """
    import json

    manifesto = {
        "pipeline":        "ingest_bronze",
        "versao":          "1.0.0",
        "data_execucao":   datetime.now().isoformat(),
        "particao":        particao,
        "fonte":           "postgresql://financeiro_source",
        "destino":         str(BRONZE_PATH),
        "tabelas":         resultados,
        "total_tabelas":   len(resultados),
        "total_sucesso":   sum(1 for r in resultados if r["status"] == "sucesso"),
        "total_erro":      sum(1 for r in resultados if r["status"] == "erro"),
        "total_linhas":    sum(r.get("linhas", 0) for r in resultados),
    }

    arquivo = BRONZE_PATH / f"_manifesto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(arquivo, "w", encoding="utf-8") as f:
        json.dump(manifesto, f, indent=2, ensure_ascii=False, default=str)

    logger.info(f"Manifesto gravado em: {arquivo}")
    return manifesto

# ----------------------------------------------------------------
# Main
# ----------------------------------------------------------------
def main():
    logger.info("=" * 55)
    logger.info("PIPELINE DE INGESTÃO — BRONZE LAYER")
    logger.info("Equivalente: Azure Data Factory Copy Activity")
    logger.info("=" * 55)

    # Cria estrutura do Data Lake
    criar_estrutura_data_lake()

    # Conecta ao PostgreSQL
    logger.info("Conectando ao PostgreSQL (fonte)...")
    engine = create_engine(DB_URL)

    # Testa conexão
    with engine.connect() as conn:
        resultado = conn.execute(text("SELECT version()"))
        logger.success(f"Conectado: {resultado.fetchone()[0][:50]}...")

    # Partição da carga
    particao = gerar_particao()
    logger.info(f"Partição: {particao}")

    # Ingere todas as tabelas
    resultados = []
    for tabela in TABELAS:
        resultado = ingerir_tabela(engine, tabela, particao)
        resultados.append(resultado)

    # Gera manifesto
    manifesto = gerar_manifesto(resultados, particao)

    # Resumo final
    logger.info("=" * 55)
    logger.info("RESUMO DA INGESTÃO")
    logger.info("=" * 55)
    logger.success(f"Tabelas processadas: {manifesto['total_tabelas']}")
    logger.success(f"Sucessos:            {manifesto['total_sucesso']}")
    logger.success(f"Erros:               {manifesto['total_erro']}")
    logger.success(f"Total de linhas:     {manifesto['total_linhas']:,}")
    logger.info("=" * 55)

    if manifesto["total_erro"] > 0:
        logger.warning("Algumas tabelas falharam. Verifique o manifesto.")
    else:
        logger.success("Pipeline Bronze concluído com sucesso!")

if __name__ == "__main__":
    main()
