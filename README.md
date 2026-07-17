# ERP Sales Lakehouse

> Pipeline de dados completo simulando a modernização analítica de um ERP de vendas: de um SQL Server transacional até um Lakehouse em camadas (Bronze/Silver/Gold/Diamond) consumido via Power BI.

**Status:** 🚧 Em desenvolvimento — as 4 camadas de dados (Bronze/Silver/Gold/Diamond) já rodam ponta a ponta localmente, com testes automatizados e infraestrutura AWS pronta como código (não aplicada). Faltam o dashboard Power BI e a documentação final.

*[English version](README.en.md)*

---

## 1. Visão geral

Este projeto simula o cenário de uma empresa com um ERP de vendas em **SQL Server**, que precisa se tornar uma plataforma analítica moderna baseada em **Lakehouse** na AWS + Databricks.

O objetivo é demonstrar, de ponta a ponta, competências de Engenharia de Dados:

- extração incremental de um banco transacional;
- ingestão e versionamento de dados brutos em Data Lake (S3);
- processamento distribuído com PySpark/Databricks;
- modelagem em camadas (Medallion Architecture: Bronze → Silver → Gold → Diamond);
- catalogação de metadados (Glue) e consulta ad-hoc (Athena);
- modelagem dimensional para consumo em BI (Power BI);
- qualidade de dados, governança e boas práticas de engenharia de software aplicadas a dados.

## 2. Cenário de negócio

A área comercial de uma empresa fictícia de vendas precisa acompanhar indicadores como receita bruta/líquida, ticket médio, margem, devoluções, meta x realizado, e rankings de produtos/clientes/vendedores — hoje presos em um ERP transacional sem camada analítica.

As entidades de origem são: clientes, produtos, vendedores, notas fiscais (cabeçalho + itens), devoluções, metas comerciais, formas de pagamento e regiões de venda.

Regras de negócio detalhadas estão em [`docs/business_rules.md`](docs/business_rules.md).

## 3. Arquitetura

```
SQL Server (ERP)
      │  extração incremental (Python/PySpark via JDBC — watermark em UpdatedAt)
      ▼
AWS S3 — camada Bronze (Delta Lake)
      │  processamento no Databricks (PySpark)
      ▼
Delta Lake — Bronze → Silver → Gold → Diamond
      │
      ├──► Glue Data Catalog (metadados) ──► Athena (consulta SQL ad-hoc)
      │
      ▼
Power BI (consumo analítico / dashboards executivos)

GitHub — versionamento de código e documentação em todas as etapas
```

Descrição completa da arquitetura, das camadas e das decisões técnicas em [`docs/architecture.md`](docs/architecture.md).

## 4. Stack tecnológica

| Camada | Tecnologia |
|---|---|
| Origem transacional | SQL Server |
| Extração | Python + PySpark (JDBC) |
| Data Lake | AWS S3 |
| Segurança/Permissões | AWS IAM |
| Processamento | Databricks + PySpark |
| Formato de tabelas | Delta Lake |
| Catálogo de metadados | AWS Glue Data Catalog |
| Consulta SQL sobre o Lake | AWS Athena |
| Visualização | Power BI |
| Ambiente de desenvolvimento | VS Code |
| Versionamento | GitHub |

## 5. Camadas de dados (Medallion Architecture)

| Camada | Propósito |
|---|---|
| **Bronze** | Dados brutos do ERP, fiéis à origem, com colunas técnicas de controle (`ingestion_timestamp`, `source_system`, `source_table`, `batch_id`, `load_type`). Sem regra de negócio. |
| **Silver** | Dados limpos, tipados, deduplicados e validados (PK/FK, nulos, datas, valores negativos indevidos). Granularidade transacional preservada. |
| **Gold** | Modelo dimensional (star schema) pronto para BI: `dim_customer`, `dim_product`, `dim_salesperson`, `dim_date`, `dim_payment_method`, `dim_region`, `fact_sales`, `fact_returns`, `fact_sales_targets`. |
| **Diamond** | Agregados executivos prontos para consumo direto no Power BI (KPIs comerciais, rankings, meta x realizado), reduzindo processamento no relatório. |

## 6. Estrutura do repositório

```
erp-sales-lakehouse/
├── README.md
├── requirements.txt
├── .gitignore
├── .env.example
├── docs/                  # arquitetura, regras de negócio, dicionário de dados
├── sql/                   # scripts para simular o ERP de origem (SQL Server)
├── notebooks/             # pipeline Databricks (ingestão → bronze → silver → gold → diamond)
├── src/                   # código reutilizável (extração, qualidade, transformações, config)
├── tests/                 # testes de qualidade e transformação (pytest)
├── infra/terraform/       # infraestrutura AWS como código (S3, IAM, Glue, Athena) — pronta, não aplicada
├── powerbi/               # dashboard e documentação do modelo de BI
└── diagrams/              # diagramas de arquitetura
```

## 7. Roadmap do projeto

- [x] Estrutura inicial do repositório
- [x] Modelagem e scripts SQL do ERP de origem
- [x] Extração incremental (Python/PySpark JDBC → Bronze, watermark em `UpdatedAt`, MERGE idempotente)
- [x] Transformações Silver (limpeza, validação, quarentena de registros rejeitados)
- [x] Modelagem dimensional Gold (star schema, surrogate keys, fórmulas de receita/margem)
- [x] Agregados Diamond (KPIs, rankings, meta x realizado, ticket médio)
- [x] Testes de qualidade automatizados (pytest, `src/quality.py` e `src/transformations.py`)
- [x] Infraestrutura AWS como código (Terraform: S3/IAM/Glue/Athena) — validada, não aplicada (ver [`infra/terraform/README.md`](infra/terraform/README.md))
- [x] Documentação Power BI (conexão, DAX, layout de páginas) + export local dos dados
- [x] Documentação final (dicionário de dados, regras de negócio, arquitetura) + versão em inglês
- [x] Automação diária local (Windows Task Scheduler: simula atividade do ERP + roda o pipeline sozinho todo dia — ver [`docs/automation.md`](docs/automation.md))
- [ ] Dashboard Power BI (o `.pbix` em si)
- [ ] Post para LinkedIn

## 8. Como executar (local)

Pipeline hoje roda 100% local (`RUN_MODE=local`), sem depender de AWS:

```bash
# 1. Ambiente Python (requer Python 3.11 — PySpark quebra no Windows com 3.12+, ver requirements.txt)
python -m venv .venv
.venv/Scripts/pip install pyspark==3.5.3 delta-spark==3.2.1 python-dotenv pyodbc

# 2. Banco de origem (SQL Server local — ver sql/00 a sql/04, nessa ordem)
sqlcmd -S <seu_servidor> -E -C -i sql/00_create_database.sql
sqlcmd -S <seu_servidor> -E -C -i sql/01_create_tables.sql
sqlcmd -S <seu_servidor> -E -C -f 65001 -i sql/02_insert_sample_data.sql

# 3. .env — copiar .env.example para .env e preencher (SQL Server local + RUN_MODE=local)

# 4. Pipeline completo, em ordem
.venv/Scripts/python notebooks/01_ingest_bronze.py
.venv/Scripts/python notebooks/02_transform_silver.py
.venv/Scripts/python notebooks/03_model_gold.py
.venv/Scripts/python notebooks/04_create_diamond.py

# 5. Testes
.venv/Scripts/python -m pip install pytest
.venv/Scripts/python -m pytest tests/ -v
```

Detalhes de setup do SQL Server (TCP/IP, login dedicado) em `sql/04_create_pipeline_login.sql`. Para migrar para AWS, ver [`infra/terraform/README.md`](infra/terraform/README.md).

## 9. Notas técnicas que valem destaque

Alguns problemas reais encontrados e corrigidos ao construir isso — do tipo que só aparece quando você roda o pipeline de ponta a ponta de verdade, não só escreve o código:

- **PySpark quebra no Windows com Python 3.12+** (bug conhecido, SPARK-53759) — o processo worker Python morre silenciosamente, sem traceback. Diagnosticado e corrigido fixando o projeto em Python 3.11.
- **`DATETIME2(7)` do SQL Server vs. `TimestampType` do Spark (microssegundo)**: comparação ingênua de watermark fazia a carga incremental da Bronze reprocessar as mesmas linhas para sempre. Corrigido truncando os dois lados da comparação para o segundo inteiro.
- **Estouro de `DATEDIFF(SECOND, 0, ...)`** no SQL Server para qualquer data a mais de ~68 anos de 1900-01-01 — corrigido ancorando o cálculo numa data recente em vez da epoch.
- **Glue Crawler precisa de `delta_target`, não `s3_target` genérico**, para entender o `_delta_log/` do Delta Lake corretamente — do contrário o crawler (ou, localmente, o conector de Parquet do Power BI) lê todo arquivo parquet físico, incluindo os que um `overwrite` do Delta já removeu logicamente mas ainda não foram limpos via `VACUUM`, duplicando dado silenciosamente.

---

*[English version](README.en.md)*
