# Post para LinkedIn — ERP Sales Lakehouse

> Pronto para publicar. Falta só: (1) inserir o link do repositório onde está `[LINK DO REPOSITÓRIO]`, (2) opcionalmente anexar 1-2 prints do dashboard Power BI quando o `.pbix` estiver montado (o post funciona sem eles, mas ganha). Duas versões abaixo: português (principal) e inglês (mais curta, para o alcance internacional).

---

## Versão em português

🏗️ Passei as últimas semanas construindo um projeto de engenharia de dados de ponta a ponta: um **Lakehouse completo para dados de vendas**, simulando a modernização analítica de um ERP real.

O cenário: uma empresa de vendas com dados presos num SQL Server transacional, sem camada analítica. O objetivo: transformar isso numa plataforma de dados moderna, com arquitetura em camadas, qualidade de dados de verdade e infraestrutura como código.

**O que construí:**

🔹 Extração incremental (SQL Server → PySpark via JDBC), com watermark e MERGE idempotente
🔹 Arquitetura Medallion: Bronze → Silver → Gold → Diamond, cada camada com responsabilidade própria
🔹 Camada de qualidade de dados com quarentena de registros rejeitados (nada é descartado silenciosamente)
🔹 Modelo dimensional (star schema) com surrogate keys e fórmulas de negócio padronizadas
🔹 Agregados executivos pré-calculados para consumo direto no Power BI
🔹 Infraestrutura AWS como código (Terraform: S3, IAM least-privilege, Glue, Athena)
🔹 31 testes automatizados (pytest), incluindo prova de regressão real

**Alguns números:**

- 9 tabelas de origem, ~2 mil registros transacionais processados
- 4 camadas de dados, todas validadas rodando de ponta a ponta (não só "no papel")
- 3 registros propositalmente inconsistentes plantados nos dados — todos capturados corretamente pela camada de qualidade, com o motivo exato da rejeição

**O que mais aprendi não foi a parte "feliz":**

Rodar o pipeline de verdade contra dados reais (mesmo sintéticos) expõe problemas que documentação nenhuma antecipa. Alguns exemplos que virei case ao longo do projeto:

- Um bug clássico de precisão entre `DATETIME2` do SQL Server e o `Timestamp` do Spark que fazia a carga incremental reprocessar tudo pra sempre
- PySpark quebrando silenciosamente no Windows com Python 3.12+ (sem traceback nenhum — só descobri comparando ambiente por ambiente)
- Um crawler do Glue mal configurado que duplicaria dados do Power BI ao ler Delta Lake sem entender o `_delta_log`

Esse tipo de coisa não aparece em tutorial — só aparece quando você roda de verdade, valida os números e não aceita "parece que funcionou".

Projeto completo, com documentação de arquitetura, dicionário de dados e todas as decisões de modelagem justificadas: [LINK DO REPOSITÓRIO]

```text
#EngenhariaDeDados #DataEngineering #PySpark #Databricks #AWS #Terraform #PowerBI #SQLServer #DeltaLake
```

---

## English version

🏗️ Spent the last few weeks building a full end-to-end data engineering project: a **complete sales data Lakehouse**, simulating the analytics modernization of a real ERP.

SQL Server → incremental PySpark extraction (JDBC, watermark, idempotent MERGE) → Medallion architecture (Bronze/Silver/Gold/Diamond) → quarantine-based data quality (nothing silently dropped) → dimensional model with surrogate keys → executive aggregates pre-computed for Power BI → AWS infrastructure as code (Terraform, least-privilege IAM) → 31 automated tests.

The real learning wasn't the happy path — it was the bugs only real execution surfaces: a `DATETIME2` vs. Spark `Timestamp` precision mismatch that made incremental loads reprocess forever, PySpark silently dying on Windows with Python 3.12+, a Glue crawler misconfiguration that would've duplicated data reading Delta Lake the wrong way. None of that shows up in a tutorial — only in running the thing for real and refusing to accept "looks like it worked."

Full repo, with architecture docs, data dictionary, and every modeling trade-off documented: [REPO LINK]

```text
#DataEngineering #PySpark #Databricks #AWS #Terraform #PowerBI #SQLServer #DeltaLake
```
