# ERP Sales Lakehouse

> End-to-end data pipeline simulating the analytics modernization of a sales ERP: from a transactional SQL Server database to a layered Lakehouse (Bronze/Silver/Gold/Diamond) consumed through Power BI.

**Status:** 🚧 In progress — all 4 data layers (Bronze/Silver/Gold/Diamond) run end-to-end locally, with automated tests and AWS infrastructure ready as code (not deployed). Remaining: Power BI dashboard build and final documentation polish.

*[Versão em português](README.md)*

---

## 1. Overview

This project simulates a company with a sales ERP running on **SQL Server** that needs to become a modern analytics platform on a **Lakehouse** architecture (AWS + Databricks).

The goal is to demonstrate, end to end, Data Engineering skills:

- incremental extraction from a transactional database;
- raw data ingestion and versioning in a Data Lake (S3);
- distributed processing with PySpark/Databricks;
- layered modeling (Medallion Architecture: Bronze → Silver → Gold → Diamond);
- metadata cataloging (Glue) and ad-hoc querying (Athena);
- dimensional modeling for BI consumption (Power BI);
- data quality, governance, and software engineering best practices applied to data.

## 2. Business scenario

The commercial team of a fictional sales company needs to track KPIs such as gross/net revenue, average ticket, margin, returns, target vs. actual, and product/customer/salesperson rankings — currently locked inside a transactional ERP with no analytics layer.

Source entities: customers, products, salespeople, sales invoices (header + items), returns, sales targets, payment methods, and sales regions.

Detailed business rules: [`docs/business_rules.md`](docs/business_rules.md).

## 3. Architecture

```
SQL Server (ERP)
      │  incremental extraction (Python/PySpark via JDBC — watermark on UpdatedAt)
      ▼
AWS S3 — Bronze layer (Delta Lake)
      │  processing on Databricks (PySpark)
      ▼
Delta Lake — Bronze → Silver → Gold → Diamond
      │
      ├──► Glue Data Catalog (metadata) ──► Athena (ad-hoc SQL queries)
      │
      ▼
Power BI (analytics / executive dashboards)

GitHub — code and documentation versioning at every stage
```

Full architecture description, layer-by-layer decisions, and trade-offs: [`docs/architecture.md`](docs/architecture.md).

**Current execution mode:** the pipeline runs 100% locally today (`RUN_MODE=local`), with Delta Lake on disk instead of S3 — a single config switch (`src/config.py`) changes the destination without touching transformation logic. AWS infrastructure already exists as validated Terraform code (`infra/terraform/`) but has **not** been applied against a real account — see [`infra/terraform/README.md`](infra/terraform/README.md) for the deployment checklist.

## 4. Tech stack

| Layer | Technology |
|---|---|
| Transactional source | SQL Server |
| Extraction | Python + PySpark (JDBC) |
| Data Lake | AWS S3 |
| Security/Permissions | AWS IAM |
| Processing | Databricks + PySpark |
| Table format | Delta Lake |
| Metadata catalog | AWS Glue Data Catalog |
| SQL query engine over the Lake | AWS Athena |
| Visualization | Power BI |
| Development environment | VS Code |
| Version control | GitHub |
| Infrastructure as Code | Terraform |
| Automated testing | pytest |

## 5. Data layers (Medallion Architecture)

| Layer | Purpose | Validated |
|---|---|---|
| **Bronze** | Raw ERP data, faithful to the source, with technical control columns (`ingestion_timestamp`, `source_system`, `source_table`, `batch_id`, `load_type`). No business rules applied. Incremental load via watermark (`UpdatedAt`) with idempotent `MERGE`. | 9 tables, ~2,086 rows |
| **Silver** | Clean, typed, deduplicated, validated data (PK/FK, nulls, dates, invalid negative values). Transactional grain preserved. Rejected records go to quarantine tables with a logged reason, never silently dropped. | 9 tables, 3 rows quarantined (proven against intentionally dirty seed data) |
| **Gold** | Dimensional model (star schema) ready for BI: `dim_customer`, `dim_product`, `dim_salesperson`, `dim_date`, `dim_payment_method`, `dim_region`, `fact_sales`, `fact_returns`, `fact_sales_targets`. Own surrogate keys, standardized revenue/margin formulas. | 6 dimensions + 3 facts, 1,302/95/120 rows |
| **Diamond** | Executive aggregates ready for direct Power BI consumption (commercial KPIs, rankings, target vs. actual), cutting report-time processing. | 6 tables: monthly sales, product/customer/salesperson rankings, target vs. actual, commercial KPIs |

## 6. Repository structure

```
erp-sales-lakehouse/
├── README.md / README.en.md
├── requirements.txt
├── .gitignore
├── .env.example
├── docs/                  # architecture, business rules, data dictionary
├── sql/                   # scripts simulating the source ERP (SQL Server)
├── notebooks/             # pipeline (ingest → bronze → silver → gold → diamond)
├── src/                   # reusable code (extraction, quality, transformations, config)
├── tests/                 # quality and transformation tests (pytest)
├── infra/terraform/       # AWS infrastructure as code (S3, IAM, Glue, Athena) — ready, not applied
├── powerbi/               # dashboard source and BI model documentation
└── diagrams/              # architecture diagrams
```

## 7. Project roadmap

- [x] Initial repository structure
- [x] Source ERP modeling and SQL scripts
- [x] Incremental extraction (Python/PySpark JDBC → Bronze, watermark on `UpdatedAt`, idempotent `MERGE`)
- [x] Silver transformations (cleaning, validation, quarantine of rejected records)
- [x] Gold dimensional modeling (star schema, surrogate keys, revenue/margin formulas)
- [x] Diamond aggregates (KPIs, rankings, target vs. actual, average ticket)
- [x] Automated quality tests (pytest, `src/quality.py` and `src/transformations.py`)
- [x] AWS infrastructure as code (Terraform: S3/IAM/Glue/Athena) — validated, not applied (see [`infra/terraform/README.md`](infra/terraform/README.md))
- [x] Power BI documentation (connection paths, DAX measures, page plan) + local data export
- [ ] Power BI dashboard (`.pbix` file itself)
- [ ] Final documentation polish
- [ ] LinkedIn post

## 8. How to run (local)

The pipeline runs 100% locally today (`RUN_MODE=local`), with no AWS dependency:

```bash
# 1. Python environment (requires Python 3.11 — PySpark breaks on Windows with 3.12+, see requirements.txt)
python -m venv .venv
.venv/Scripts/pip install pyspark==3.5.3 delta-spark==3.2.1 python-dotenv pyodbc

# 2. Source database (local SQL Server — run sql/00 through sql/04, in order)
sqlcmd -S <your_server> -E -C -i sql/00_create_database.sql
sqlcmd -S <your_server> -E -C -i sql/01_create_tables.sql
sqlcmd -S <your_server> -E -C -i sql/02_insert_sample_data.sql

# 3. .env — copy .env.example to .env and fill it in (local SQL Server + RUN_MODE=local)

# 4. Full pipeline, in order
.venv/Scripts/python notebooks/01_ingest_bronze.py
.venv/Scripts/python notebooks/02_transform_silver.py
.venv/Scripts/python notebooks/03_model_gold.py
.venv/Scripts/python notebooks/04_create_diamond.py

# 5. Tests
.venv/Scripts/python -m pip install pytest
.venv/Scripts/python -m pytest tests/ -v

# 6. Power BI data export (see powerbi/README.md for the full guide)
.venv/Scripts/python powerbi/export_snapshot.py
```

SQL Server setup details (TCP/IP, dedicated login) in `sql/04_create_pipeline_login.sql`. To migrate to AWS, see [`infra/terraform/README.md`](infra/terraform/README.md).

## 9. Engineering notes worth highlighting

A few non-obvious issues found and fixed while building this — the kind of thing that only shows up when you actually run the pipeline end to end instead of just writing code:

- **PySpark on Windows breaks with Python 3.12+** (known upstream issue, SPARK-53759) — the Python worker process dies silently with no traceback. Root-caused and fixed by pinning the project to Python 3.11.
- **`DATETIME2(7)` vs. Spark's microsecond-precision `TimestampType`**: naive watermark comparison caused the incremental Bronze load to re-fetch already-processed rows forever. Fixed by truncating both sides of the comparison to whole seconds.
- **`DATEDIFF(SECOND, 0, ...)` integer overflow** in SQL Server for any date more than ~68 years from 1900-01-01 — fixed by anchoring the diff to a recent date instead of the epoch.
- **Glue Crawlers need `delta_target`, not a generic `s3_target`**, to correctly parse Delta Lake's `_delta_log/` — otherwise the crawler (or, locally, Power BI's Parquet connector) reads every physical parquet file including ones a Delta `overwrite` has logically removed but not yet `VACUUM`ed, silently duplicating data.

## 10. Author's note

Built as a hands-on portfolio project to demonstrate full-pipeline data engineering judgment — not just writing transformation code, but running everything against real data, catching real bugs, and documenting the trade-offs behind every non-obvious modeling decision (header vs. item discount allocation, region semantics in `fact_sales`, cancelled-invoice exclusion in Diamond, and more — see `docs/data_dictionary.md` and `docs/business_rules.md`).
