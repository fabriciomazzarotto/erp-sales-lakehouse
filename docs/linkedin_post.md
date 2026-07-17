# Post para LinkedIn — ERP Sales Lakehouse

> Pronto para publicar. O `.pbix` em si não foi montado nesta fase do projeto (decisão consciente — o repositório já demonstra a engenharia de dados ponta a ponta, incluindo a camada pronta para consumo em BI). Duas versões abaixo: português (principal) e inglês (mais curta, para o alcance internacional).

---

## Versão em português

🏗️ Passei as últimas semanas construindo um projeto de engenharia de dados de ponta a ponta: um **Lakehouse completo para dados de vendas**, simulando a modernização analítica de um ERP real — e fazendo ele rodar sozinho, todo dia, de verdade.

O cenário: uma empresa de vendas com dados presos num SQL Server transacional, sem camada analítica. O objetivo: transformar isso numa plataforma de dados moderna, com arquitetura em camadas, qualidade de dados de verdade, automação real e infraestrutura como código.

**O que construí:**

🔹 Extração incremental (SQL Server → PySpark via JDBC), com watermark e MERGE idempotente
🔹 Arquitetura Medallion: Bronze → Silver → Gold → Diamond, cada camada com responsabilidade própria
🔹 Camada de qualidade de dados com quarentena de registros rejeitados (nada é descartado silenciosamente)
🔹 Modelo dimensional (star schema) com surrogate keys e fórmulas de negócio padronizadas
🔹 Agregados executivos (Diamond) publicados num banco SQL Server dedicado — dá ao Power BI um conector nativo real, com caminho de atualização agendada via Gateway de Dados Local, sem depender de nuvem paga
🔹 **Automação diária de verdade**: duas tarefas no Windows Task Scheduler — uma simula atividade nova no ERP (notas, devoluções, atualizações de cadastro), a outra reprocessa o pipeline inteiro sozinha, todo dia, sem eu abrir nada
🔹 Infraestrutura AWS como código (Terraform: S3, IAM least-privilege, Glue, Athena), pronta para o dia da migração
🔹 Testes automatizados (pytest) cobrindo as regras de qualidade e transformação

**O que mais aprendi não foi a parte "feliz":**

Rodar o pipeline de verdade, repetidamente, contra dados "de agora" (não só um seed histórico) expõe problemas que documentação nenhuma antecipa. Alguns viraram case ao longo do projeto:

- Um bug clássico de precisão entre `DATETIME2` do SQL Server e o `Timestamp` do Spark que fazia a carga incremental reprocessar tudo pra sempre
- Um bug de timezone: o Spark local interpretando timestamps UTC do SQL Server como hora local, deslocando 3h e jogando notas recém-criadas na quarentena por "data futura"
- Um bug do próprio SQL Server 2025 no predicado de extração incremental — só reproduzível comparando os valores como inteiro, não como data reconstruída
- Um bug de codificação (mojibake) nos dados de origem, causado por rodar scripts UTF-8 via `sqlcmd` sem especificar a code page — como a automação diária rodava o script todo dia, o bug corromperia dado novo continuamente até eu corrigir a causa raiz (não só o histórico)

Nenhum desses aparece em tutorial — só aparece quando você roda de verdade, repetidamente, e não aceita "parece que funcionou".

Projeto completo, com documentação de arquitetura, dicionário de dados, automação e todas as decisões de modelagem justificadas: https://github.com/fabriciomazzarotto/erp-sales-lakehouse

```text
#EngenhariaDeDados #DataEngineering #PySpark #Databricks #AWS #Terraform #PowerBI #SQLServer #DeltaLake
```

---

## English version

🏗️ Spent the last few weeks building a full end-to-end data engineering project: a **complete sales data Lakehouse**, simulating the analytics modernization of a real ERP — and making it run on its own, every day, for real.

SQL Server → incremental PySpark extraction (JDBC, watermark, idempotent MERGE) → Medallion architecture (Bronze/Silver/Gold/Diamond) → quarantine-based data quality (nothing silently dropped) → dimensional model with surrogate keys → executive aggregates published to a dedicated SQL Server database (a real native connector path for Power BI scheduled refresh via a local gateway, no paid cloud required) → **real daily automation** (Windows Task Scheduler simulates new ERP activity and reprocesses the whole pipeline on its own, every day) → AWS infrastructure as code (Terraform, least-privilege IAM) → automated tests.

The real learning wasn't the happy path — it was the bugs only real, repeated execution surfaces: a `DATETIME2` vs. Spark `Timestamp` precision mismatch that made incremental loads reprocess forever, a timezone bug where local Spark misread SQL Server's UTC timestamps as local time, a SQL Server 2025 predicate quirk, and an encoding bug that would have kept corrupting new data every single day if I'd only patched the symptom instead of the root cause. None of that shows up in a tutorial — only in running the thing for real, repeatedly, and refusing to accept "looks like it worked."

Full repo, with architecture docs, data dictionary, automation write-up, and every modeling trade-off documented: https://github.com/fabriciomazzarotto/erp-sales-lakehouse

```text
#DataEngineering #PySpark #Databricks #AWS #Terraform #PowerBI #SQLServer #DeltaLake
```
