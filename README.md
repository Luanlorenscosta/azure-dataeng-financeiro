# 🏦 Azure Data Engineering Portfolio — Fintech Financeiro

![Python](https://img.shields.io/badge/Python-3.11+-blue?logo=python)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-black?logo=apachekafka)
![DuckDB](https://img.shields.io/badge/DuckDB-1.5-yellow?logo=duckdb)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)
![CI/CD](https://img.shields.io/badge/CI%2FCD-GitHub_Actions-green?logo=githubactions)
![Tests](https://img.shields.io/badge/Tests-15%2F15_passing-brightgreen?logo=pytest)

> Portfólio completo de Engenharia de Dados simulando o ambiente de dados de uma **fintech brasileira**, com pipelines de ingestão, transformação, streaming em tempo real e detecção de fraudes.

## 🎯 Visão Geral

- **50.000 transações financeiras** sintéticas com ~978 casos de fraude
- **Arquitetura Medallion** (Bronze → Silver → Gold)
- **Streaming em tempo real** com detecção de fraudes via Kafka
- **Data Warehouse analítico** com DuckDB
- **CI/CD** com GitHub Actions e 15 testes automatizados

## 🛠️ Stack Tecnológica

| Componente | Local (Portfólio) | Azure Equivalente |
|---|---|---|
| Orquestração | Scripts Python | Azure Data Factory |
| Processamento | Pandas + DuckDB | Azure Databricks |
| Data Lake | Parquet local | ADLS Gen2 |
| Data Warehouse | DuckDB | Synapse Analytics |
| Streaming | Apache Kafka | Azure Event Hub |
| Banco Fonte | PostgreSQL Docker | Azure SQL Database |
| CI/CD | GitHub Actions | Azure DevOps |

## 🚀 Como Executar

### 1. Clone e instale dependências
```bash
git clone https://github.com/Luanlorenscosta/azure-dataeng-financeiro.git
cd azure-dataeng-financeiro
pip install -r requirements.txt
pip install kafka-python
```

### 2. Suba o ambiente Docker
```bash
docker-compose up -d
```

### 3. Crie as tabelas e gere os dados
```bash
docker cp sql/ddl/01_create_tables.sql financeiro-postgres:/tmp/
docker exec financeiro-postgres psql -U postgres -d financeiro_source -f /tmp/01_create_tables.sql
python sql/seed_data/generate_fake_data.py
```

### 4. Execute os pipelines
```bash
python pipelines/adf/01_ingest_bronze.py
python pipelines/databricks/01_bronze_to_silver.py
python pipelines/databricks/02_silver_to_gold.py
```

### 5. Execute o streaming (dois terminais)
```bash
python streaming/event_hub_consumer.py  # Terminal 1
python streaming/event_hub_producer.py  # Terminal 2
```

## 📊 Resultados e Insights

| Métrica | Valor |
|---|---|
| Total de transações | 50.000 |
| Clientes únicos | 500 |
| Taxa de fraude | ~1,95% |
| Estado com mais fraude | MG (2,46%) |
| Horário de maior risco | 8h (2,47% fraude) |
| Melhor dia em volume | R$ 298.690 |

## ⚡ Regras de Detecção de Fraude (Streaming)

| Regra | Descrição | Severidade |
|---|---|---|
| SCORE_FRAUDE_ALTO | Score > 0.65 | ALTA |
| VALOR_SUSPEITO | Valor > R$ 5.000 | MÉDIA |
| VELOCITY_CHECK | +5 transações/minuto por conta | ALTA |
| STATUS_SUSPEITA | Marcado como suspeito na origem | ALTA |

## ✅ Testes
```bash
pytest tests/ -v  # 15/15 passing
```

## 👨‍💻 Autor

**Luan Lorens da Costa** — DBA Sênior | Oracle · PostgreSQL · SQL Server | Cloud & Data Engineering

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Luan_Lorens-blue?logo=linkedin)](https://www.linkedin.com/in/luan-lorens-da-costa/)

**Certificações:** AZ-900 | DP-900 | DP-300 | OCP (em andamento)
