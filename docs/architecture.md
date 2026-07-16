# Arquitetura — ERP Sales Lakehouse

## 1. Visão de alto nível

```
┌─────────────────┐
│   SQL Server     │  Origem transacional (OLTP) — schema erp.*
│   (ERP_Sales)    │  Ver sql/01_create_tables.sql
└────────┬─────────┘
         │ Extração incremental (Python/PySpark via JDBC)
         │ watermark: coluna UpdatedAt de cada tabela
         ▼
┌─────────────────────────────────────────────┐
│              AWS S3 — Bronze                 │
│  s3://erp-sales-lakehouse-bronze/            │
│    erp_customers/  erp_products/  ...        │
│  Formato: Delta Lake                         │
│  + colunas técnicas (ingestion_timestamp,     │
│    source_system, source_table, batch_id,     │
│    load_type)                                │
└────────┬──────────────────────────────────────┘
         │ Processamento no Databricks (PySpark)
         ▼
┌─────────────────────────────────────────────┐
│              AWS S3 — Silver                 │
│  Dados limpos, tipados, deduplicados,        │
│  validados (PK/FK, datas, valores)           │
└────────┬──────────────────────────────────────┘
         │ Modelagem dimensional (PySpark)
         ▼
┌─────────────────────────────────────────────┐
│              AWS S3 — Gold                   │
│  Star schema: dim_* e fact_*                 │
└────────┬──────────────────────────────────────┘
         │ Agregação para consumo executivo
         ▼
┌─────────────────────────────────────────────┐
│              AWS S3 — Diamond                │
│  KPIs, rankings, meta x realizado (agregado) │
└────────┬──────────────────────────────────────┘
         │
         ├──► AWS Glue Data Catalog (metadados de todas as camadas)
         │         │
         │         ▼
         │    AWS Athena (consulta SQL ad-hoc / validação)
         │
         ▼
   Power BI (Import ou DirectQuery via conector, conforme etapa de BI)
```

Todo o código (SQL de origem, notebooks Databricks, módulos `src/`, testes) é versionado no **GitHub**, com desenvolvimento em **VS Code**.

**Status atual de execução:** o pipeline roda hoje 100% local (`RUN_MODE=local` no `.env`), com Delta Lake em disco (`./data/lakehouse/<camada>/`) em vez de S3, via `src.config.get_layer_path`/`get_table_path`. A troca para `RUN_MODE=aws` é só uma mudança de configuração — a lógica de transformação não muda. A infraestrutura AWS (buckets, IAM, Glue, Athena) já existe como código Terraform validado (`infra/terraform/`), mas **não foi aplicada** contra uma conta real — ver `infra/terraform/README.md` para o porquê e o checklist de quando for aplicar.

## 2. Fluxo por camada

| Etapa | Onde roda | Entrada | Saída |
|---|---|---|---|
| Extração | Python/PySpark (JDBC) | SQL Server (`erp.*`) | Arquivos Delta em S3 Bronze |
| Bronze → Silver | Databricks (PySpark) | Delta Bronze | Delta Silver |
| Silver → Gold | Databricks (PySpark) | Delta Silver | Delta Gold (dimensional) |
| Gold → Diamond | Databricks (PySpark/SQL) | Delta Gold | Delta Diamond (agregado) |
| Catalogação | Glue Crawler / Glue Data Catalog | Delta (todas as camadas) | Metadados/tabelas no catálogo |
| Consulta ad-hoc | Athena | Glue Data Catalog | Resultado de queries SQL |
| Consumo final | Power BI | Gold/Diamond (via catálogo ou conector Delta) | Dashboards |

## 3. Por que Medallion + Diamond (e não só Bronze/Silver/Gold)?

A arquitetura Medallion clássica (Bronze/Silver/Gold) já resolve a maior parte do problema: dados brutos → dados confiáveis → dados modelados para BI. A camada **Diamond** é uma extensão comum em cenários reais quando o Power BI precisa de performance máxima e o time de negócio pede indicadores já prontos (KPIs, rankings, meta x realizado). Em vez de recalcular tudo isso via DAX/Power Query a cada abertura do relatório, a Diamond entrega os agregados pré-calculados, reduzindo processamento no relatório — um trade-off clássico de "processar uma vez no Lakehouse vs. recalcular a cada consulta no BI".

## 4. Por que extração incremental via watermark (`UpdatedAt`)?

Entre as estratégias possíveis (coluna de data, ID incremental, watermark, hash de linha, MERGE), o projeto usa **watermark por `UpdatedAt`** como estratégia principal porque:

- é a mais comum em ERPs reais (quase todo ERP mantém uma coluna de auditoria de alteração);
- permite capturar tanto inserções quanto atualizações (um hash de linha ou ID incremental puro não capturaria updates);
- combina bem com `MERGE` no Delta Lake na chegada à Bronze/Silver, permitindo idempotência (reprocessar o mesmo batch não duplica dados).

Tabelas pequenas e de baixa mutação (`Regions`, `PaymentMethods`) serão carregadas em modo **full**, pois o custo de comparação incremental não compensa o ganho.

## 5. Segurança e governança

- **IAM** (implementado em `infra/terraform/iam.tf`): role de execução do pipeline restrita aos 4 buckets do Lakehouse (nunca `s3:*`/`Resource: "*"`), e role separada para o Glue Crawler (só leitura) — mesmo princípio de least privilege usado no login `erp_extractor` do SQL Server (`sql/04_create_pipeline_login.sql`).
- **Buckets separados por camada**, sem acesso público (`aws_s3_bucket_public_access_block` em todos), criptografia SSE-S3, versionamento, nomenclatura padronizada (`erp-sales-lakehouse-<camada>-<env>-<sufixo>`) — implementado em `infra/terraform/s3.tf`.
- **Glue Data Catalog** como fonte única de metadados para Athena e Power BI — 1 crawler por camada usando `delta_target` (suporte nativo a Delta Lake, não um `s3_target` genérico) — implementado em `infra/terraform/glue.tf`.
- **Athena** com workgroup dedicado e corte de custo por query (5 GB escaneados) — `infra/terraform/athena.tf`.
- Credenciais nunca versionadas — uso de `.env` local (ver `.env.example`) e de secrets no Databricks/AWS em produção; `terraform.tfvars` real também nunca é commitado (ver `.gitignore`).

## 6. Status desta etapa

Todas as 4 camadas de dados (Bronze/Silver/Gold/Diamond) estão implementadas e validadas rodando localmente. A infraestrutura AWS está pronta como código (Terraform validado com `terraform validate`, sem aplicar) em `infra/terraform/` — ver o README daquele diretório para o checklist de quando migrar de fato. Testes automatizados (`pytest`) cobrem `src/quality.py` e `src/transformations.py`. Pendente: dashboard Power BI e documentação final/post de portfólio.
