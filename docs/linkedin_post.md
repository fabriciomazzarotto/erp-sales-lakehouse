# Post para LinkedIn — ERP Sales Lakehouse

> Pronto para publicar. O `.pbix` em si não foi montado nesta fase do projeto (decisão consciente — o repositório já demonstra a engenharia de dados ponta a ponta, incluindo a camada pronta para consumo em BI). Duas versões abaixo: português (principal) e inglês (mais curta, para o alcance internacional).

---

## Versão em português

🏗️ Passei as últimas semanas construindo, sozinho, um Lakehouse de dados de vendas de ponta a ponta — simulando a modernização de um ERP real, e fazendo ele rodar sozinho, todo dia, de verdade.

O cenário: uma empresa de vendas com dados presos num SQL Server transacional, sem camada analítica. O objetivo: uma plataforma de dados moderna, em camadas, com qualidade de dados de verdade, automação real e infraestrutura como código.

**O que construí:**

🔹 Extração incremental (SQL Server → PySpark via JDBC), watermark + MERGE idempotente
🔹 Medallion Architecture: Bronze → Silver → Gold → Diamond, com quarentena de registros rejeitados (nada some silenciosamente)
🔹 Modelo dimensional (star schema), surrogate keys, fórmulas de negócio padronizadas
🔹 Diamond publicada num SQL Server dedicado — conector nativo pro Power BI, com caminho de atualização agendada via Gateway, sem nuvem paga
🔹 **Automação diária real**: Windows Task Scheduler simula atividade nova no ERP e reprocessa o pipeline inteiro sozinho, todo dia
🔹 Infraestrutura AWS como código (Terraform), pronta pro dia da migração
🔹 Testes automatizados (pytest)

**A parte que mais ensinou não foi a "feliz":**

Rodar de verdade, repetidamente, contra dados "de agora" (não só um seed histórico) expôs bugs que nenhum tutorial antecipa:

- `DATETIME2` (SQL Server) vs. `Timestamp` (Spark): incompatibilidade de precisão que fazia a carga incremental reprocessar tudo pra sempre
- Timezone: Spark local lendo timestamps UTC do SQL Server como hora local, deslocando 3h e jogando notas novas na quarentena por "data futura"
- Um bug do próprio SQL Server 2025 no predicado de extração incremental
- Um bug de codificação (mojibake) na origem — como a automação rodava o script gerador todo dia, ia continuar corrompendo dado novo até eu corrigir a causa raiz, não só o histórico

Nenhum desses aparece em tutorial. Só aparece rodando de verdade, repetidamente, e não aceitando "parece que funcionou".

Código aberto, com arquitetura, dicionário de dados e cada decisão de modelagem justificada: https://github.com/fabriciomazzarotto/erp-sales-lakehouse

Se você também já apanhou de um bug desses (ou tem um pior 😄), bora trocar nos comentários. E se sua empresa está contratando em dados, me chama — aberto a oportunidades.

```text
#EngenhariaDeDados #DataEngineering #PySpark #Databricks #AWS #Terraform #PowerBI #SQLServer #DeltaLake
```

---

## English version

🏗️ Spent the last few weeks building a full end-to-end data engineering project: a **complete sales data Lakehouse**, simulating the analytics modernization of a real ERP — and making it run on its own, every day, for real.

SQL Server → incremental PySpark extraction (JDBC, watermark, idempotent MERGE) → Medallion architecture (Bronze/Silver/Gold/Diamond) → quarantine-based data quality (nothing silently dropped) → dimensional model with surrogate keys → executive aggregates published to a dedicated SQL Server database (a real native connector path for Power BI scheduled refresh via a local gateway, no paid cloud required) → **real daily automation** (Windows Task Scheduler simulates new ERP activity and reprocesses the whole pipeline on its own, every day) → AWS infrastructure as code (Terraform, least-privilege IAM) → automated tests.

The real learning wasn't the happy path — it was the bugs only real, repeated execution surfaces: a `DATETIME2` vs. Spark `Timestamp` precision mismatch that made incremental loads reprocess forever, a timezone bug where local Spark misread SQL Server's UTC timestamps as local time, a SQL Server 2025 predicate quirk, and an encoding bug that would have kept corrupting new data every single day if I'd only patched the symptom instead of the root cause. None of that shows up in a tutorial — only in running the thing for real, repeatedly, and refusing to accept "looks like it worked."

Open source, with architecture docs, data dictionary, and every modeling trade-off documented: https://github.com/fabriciomazzarotto/erp-sales-lakehouse

If you've hit a bug like one of these (or a worse one 😄), let's compare notes in the comments. And if your team is hiring in data, reach out — open to opportunities.

```text
#DataEngineering #PySpark #Databricks #AWS #Terraform #PowerBI #SQLServer #DeltaLake
```
