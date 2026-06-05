# ================================================================
# 01_bronze_to_silver.py
# Transformação: Bronze → Silver
# Equivalente ao: Azure Databricks notebook (PySpark)
# O que faz:
#   - Lê os Parquet da camada Bronze
#   - Limpa e padroniza os dados
#   - Aplica regras de qualidade (Data Quality)
#   - Grava na camada Silver particionado
# Uso: python pipelines/databricks/01_bronze_to_silver.py
# ================================================================

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from loguru import logger
import json
import re

# ----------------------------------------------------------------
# Configuração dos caminhos do Data Lake
# ----------------------------------------------------------------
DATA_LAKE  = Path("data_lake")
BRONZE     = DATA_LAKE / "bronze"
SILVER     = DATA_LAKE / "silver"

# ----------------------------------------------------------------
# Funções de limpeza — cada função transforma UMA tabela
# Isso é o equivalente às transformações em PySpark no Databricks
# ----------------------------------------------------------------

def limpar_clientes(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Limpa e padroniza a tabela de clientes.
    Retorna o DataFrame limpo e um relatório de qualidade.
    """
    total_original = len(df)
    problemas = []

    # 1. Remove espaços extras dos campos de texto
    df["nome"]   = df["nome"].str.strip().str.title()
    df["email"]  = df["email"].str.strip().str.lower()
    df["cidade"] = df["cidade"].str.strip().str.title()

    # 2. Padroniza estado para maiúsculo
    df["estado"] = df["estado"].str.upper().str.strip()

    # 3. Remove pontuação do CPF (guarda só os números)
    # Em produção: aqui aplicaríamos mascaramento para LGPD
    df["cpf"] = df["cpf"].str.replace(r"[.\-]", "", regex=True)

    # 4. Valida CPF — deve ter 11 dígitos
    cpf_invalidos = df[df["cpf"].str.len() != 11]
    if len(cpf_invalidos) > 0:
        problemas.append(f"{len(cpf_invalidos)} CPFs com tamanho inválido")

    # 5. Preenche score_credito nulo com mediana
    mediana_score = df["score_credito"].median()
    nulos_score = df["score_credito"].isna().sum()
    if nulos_score > 0:
        df["score_credito"] = df["score_credito"].fillna(mediana_score)
        problemas.append(f"{nulos_score} scores preenchidos com mediana ({mediana_score:.0f})")

    # 6. Remove colunas de auditoria do Bronze (vamos recriar na Silver)
    colunas_auditoria = ["_ingestao_timestamp", "_fonte", "_tabela_origem"]
    df = df.drop(columns=[c for c in colunas_auditoria if c in df.columns])

    # 7. Adiciona metadados da camada Silver
    df["_silver_timestamp"] = datetime.now()
    df["_qualidade_ok"]     = True

    qualidade = {
        "tabela":          "clientes",
        "total_original":  total_original,
        "total_silver":    len(df),
        "removidos":       total_original - len(df),
        "problemas":       problemas,
    }

    return df, qualidade


def limpar_transacoes(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Limpa e padroniza a tabela de transações.
    É a tabela mais importante — 50.000 linhas com dados financeiros.
    """
    total_original = len(df)
    problemas = []

    # 1. Garante que data_transacao é datetime
    df["data_transacao"]     = pd.to_datetime(df["data_transacao"],     errors="coerce")
    df["data_processamento"] = pd.to_datetime(df["data_processamento"], errors="coerce")

    # 2. Verifica datas inválidas (NaT após conversão)
    datas_invalidas = df["data_transacao"].isna().sum()
    if datas_invalidas > 0:
        problemas.append(f"{datas_invalidas} datas inválidas encontradas")
        df = df.dropna(subset=["data_transacao"])

    # 3. Valores negativos não fazem sentido para transações
    valores_negativos = (df["valor"] < 0).sum()
    if valores_negativos > 0:
        problemas.append(f"{valores_negativos} valores negativos convertidos para positivo")
        df["valor"] = df["valor"].abs()

    # 4. Valores zerados são suspeitos
    valores_zero = (df["valor"] == 0).sum()
    if valores_zero > 0:
        problemas.append(f"{valores_zero} transações com valor zero")

    # 5. Padroniza campos de texto
    df["tipo"]   = df["tipo"].str.lower().str.strip()
    df["status"] = df["status"].str.lower().str.strip()
    df["canal"]  = df["canal"].str.lower().str.strip()

    # 6. Preenche score_fraude nulo com 0
    df["score_fraude"] = df["score_fraude"].fillna(0.0)

    # 7. Cria coluna derivada — faixa de valor (útil para análises)
    df["faixa_valor"] = pd.cut(
        df["valor"],
        bins=[0, 50, 200, 500, 1000, 5000, float("inf")],
        labels=["micro", "pequeno", "medio", "alto", "muito_alto", "critico"],
        right=True
    )

    # 8. Flag de transação fora do horário comercial (risco de fraude)
    df["fora_horario_comercial"] = ~df["data_transacao"].dt.hour.between(8, 18)

    # 9. Remove colunas de auditoria do Bronze
    colunas_auditoria = ["_ingestao_timestamp", "_fonte", "_tabela_origem"]
    df = df.drop(columns=[c for c in colunas_auditoria if c in df.columns])

    # 10. Metadados Silver
    df["_silver_timestamp"] = datetime.now()

    qualidade = {
        "tabela":          "transacoes",
        "total_original":  total_original,
        "total_silver":    len(df),
        "removidos":       total_original - len(df),
        "problemas":       problemas,
        "fraudes":         int(df["is_fraude"].sum()),
        "valor_total":     float(df["valor"].sum()),
        "valor_medio":     float(df["valor"].mean()),
    }

    return df, qualidade


def limpar_contas(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Limpa e padroniza a tabela de contas."""
    total_original = len(df)
    problemas = []

    # Padroniza tipo de conta
    df["tipo_conta"] = df["tipo_conta"].str.lower().str.strip()

    # Saldo nulo vira 0
    nulos_saldo = df["saldo"].isna().sum()
    if nulos_saldo > 0:
        df["saldo"] = df["saldo"].fillna(0.0)
        problemas.append(f"{nulos_saldo} saldos nulos preenchidos com 0")

    # Remove auditoria Bronze
    colunas_auditoria = ["_ingestao_timestamp", "_fonte", "_tabela_origem"]
    df = df.drop(columns=[c for c in colunas_auditoria if c in df.columns])

    df["_silver_timestamp"] = datetime.now()

    return df, {
        "tabela": "contas",
        "total_original": total_original,
        "total_silver": len(df),
        "removidos": total_original - len(df),
        "problemas": problemas,
    }


def limpar_generica(df: pd.DataFrame, nome: str) -> tuple[pd.DataFrame, dict]:
    """Limpeza genérica para tabelas menores (comerciantes, categorias)."""
    total_original = len(df)

    # Remove auditoria Bronze
    colunas_auditoria = ["_ingestao_timestamp", "_fonte", "_tabela_origem"]
    df = df.drop(columns=[c for c in colunas_auditoria if c in df.columns])

    df["_silver_timestamp"] = datetime.now()

    return df, {
        "tabela": nome,
        "total_original": total_original,
        "total_silver": len(df),
        "removidos": 0,
        "problemas": [],
    }

# ----------------------------------------------------------------
# Funções de leitura e escrita
# ----------------------------------------------------------------

def ler_bronze(tabela: str) -> pd.DataFrame:
    """
    Lê todos os arquivos Parquet de uma tabela na camada Bronze.
    Usa glob para pegar todas as partições (ano/mes/dia).
    """
    arquivos = list((BRONZE / tabela).rglob("*.parquet"))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo Parquet encontrado em bronze/{tabela}")

    dfs = [pd.read_parquet(f) for f in arquivos]
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Bronze {tabela}: {len(df):,} linhas lidas de {len(arquivos)} arquivo(s)")
    return df


def gravar_silver(df: pd.DataFrame, tabela: str):
    """
    Grava o DataFrame na camada Silver em Parquet particionado.
    """
    agora = datetime.now()
    destino = SILVER / tabela / f"ano={agora.year}/mes={agora.month:02d}/dia={agora.day:02d}"
    destino.mkdir(parents=True, exist_ok=True)

    arquivo = destino / f"{tabela}.parquet"
    df.to_parquet(arquivo, index=False, engine="pyarrow")

    tamanho_mb = arquivo.stat().st_size / (1024 * 1024)
    logger.success(
        f"  ✓ Silver {tabela}: {len(df):,} linhas | "
        f"{tamanho_mb:.2f} MB → {arquivo}"
    )

# ----------------------------------------------------------------
# Main
# ----------------------------------------------------------------
def main():
    logger.info("=" * 55)
    logger.info("PIPELINE BRONZE → SILVER")
    logger.info("Equivalente: Azure Databricks notebook")
    logger.info("=" * 55)

    relatorio_qualidade = []

    # Mapeamento tabela → função de limpeza
    transformacoes = {
        "clientes":            limpar_clientes,
        "contas":              limpar_contas,
        "transacoes":          limpar_transacoes,
    }

    # Tabelas com limpeza genérica
    tabelas_genericas = ["comerciantes", "categorias_transacao"]

    # Processa tabelas com limpeza específica
    for tabela, funcao_limpeza in transformacoes.items():
        logger.info(f"\nProcessando: {tabela}")
        try:
            df_bronze = ler_bronze(tabela)
            df_silver, qualidade = funcao_limpeza(df_bronze)
            gravar_silver(df_silver, tabela)
            relatorio_qualidade.append(qualidade)

            if qualidade["problemas"]:
                for problema in qualidade["problemas"]:
                    logger.warning(f"  ⚠ {problema}")
        except Exception as e:
            logger.error(f"  ✗ Erro em {tabela}: {e}")

    # Processa tabelas genéricas
    for tabela in tabelas_genericas:
        logger.info(f"\nProcessando: {tabela}")
        try:
            df_bronze = ler_bronze(tabela)
            df_silver, qualidade = limpar_generica(df_bronze, tabela)
            gravar_silver(df_silver, tabela)
            relatorio_qualidade.append(qualidade)
        except Exception as e:
            logger.error(f"  ✗ Erro em {tabela}: {e}")

    # Salva relatório de qualidade
    arquivo_relatorio = SILVER / f"_relatorio_qualidade_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(arquivo_relatorio, "w", encoding="utf-8") as f:
        json.dump(relatorio_qualidade, f, indent=2, ensure_ascii=False, default=str)

    # Resumo final
    logger.info("\n" + "=" * 55)
    logger.info("RESUMO BRONZE → SILVER")
    logger.info("=" * 55)
    for q in relatorio_qualidade:
        removidos = q.get("removidos", 0)
        flag = "⚠" if removidos > 0 else "✓"
        logger.success(
            f"{flag} {q['tabela']:<25} "
            f"{q['total_original']:>7,} → {q['total_silver']:>7,} linhas"
            + (f" ({removidos} removidos)" if removidos > 0 else "")
        )
    logger.success("\nPipeline Silver concluído!")

if __name__ == "__main__":
    main()
