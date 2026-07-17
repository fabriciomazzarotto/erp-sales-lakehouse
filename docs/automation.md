# Automação local — atualização diária sem AWS

> Status: **implementado e validado** via Windows Task Scheduler real (não
> só rodando os scripts manualmente). Este é o caminho de automação
> *local, sem custo de nuvem* — ver `powerbi/README.md` (seção "Atualização
> automática") para o caminho de produção (Databricks Jobs + Power BI
> Service via Athena), que exige migrar para AWS.

## Por que isso existe

Um ERP de verdade recebe dados novos todo dia. Sem isso, agendar só o
pipeline (`notebooks/01` a `04`) não demonstra nada — a extração
incremental sempre acharia "0 registros novos". Duas tarefas agendadas
resolvem isso:

1. **`ERP Sales Lakehouse - Simulate Daily Activity`** — injeta atividade
   nova no SQL Server de origem (`sql/05_simulate_daily_activity.sql`, via
   `scripts/run_daily_erp_simulation.ps1`).
2. **`ERP Sales Lakehouse - Run Pipeline`** — roda o pipeline completo
   (Bronze → Silver → Gold → Diamond → publicação em `ERP_Sales_BI` via
   `powerbi/publish_to_sql.py`, ver `powerbi/README.md`), 15 minutos depois
   da primeira, via `scripts/run_pipeline.ps1`.

## O que `sql/05_simulate_daily_activity.sql` gera por execução

- 5 a 15 novas notas fiscais (últimas 24h), com 1 a 4 itens cada
- ~15% de chance de gerar 1 devolução
- ~5% das notas nascem `Cancelada` (para a Diamond continuar tendo o que
  filtrar)
- 2 clientes com telefone atualizado + 1 produto com preço reajustado
  (`UpdatedAt` fresco — exercita o `MERGE` de **update** na Bronze, não só
  insert)

Seguro rodar todos os dias: usa `IDENTITY` para as chaves (sem colisão) e
`CHECKSUM(NEWID(), coluna_correlacionada)` para aleatoriedade por linha —
mesma técnica (e mesma pegadinha evitada) de `sql/02_insert_sample_data.sql`.

## Tarefas agendadas (Windows Task Scheduler)

| Tarefa | Horário | Script |
|---|---|---|
| `ERP Sales Lakehouse - Simulate Daily Activity` | 05:00 diário | `scripts/run_daily_erp_simulation.ps1` |
| `ERP Sales Lakehouse - Run Pipeline` | 05:15 diário | `scripts/run_pipeline.ps1` |

Registradas com `Register-ScheduledTask` (PowerShell), rodando "somente
quando o usuário estiver conectado" (não exige salvar senha do Windows).
Logs de cada execução em `logs/erp_simulation_<timestamp>.log` e
`logs/pipeline_<timestamp>.log` (pasta ignorada pelo Git).

**Comandos úteis:**

```powershell
# Ver as tarefas
Get-ScheduledTask -TaskName "ERP Sales Lakehouse*"

# Rodar manualmente agora (sem esperar o horário)
Start-ScheduledTask -TaskName "ERP Sales Lakehouse - Simulate Daily Activity"
Start-ScheduledTask -TaskName "ERP Sales Lakehouse - Run Pipeline"

# Ver o resultado da última execução (0 = sucesso)
Get-ScheduledTaskInfo -TaskName "ERP Sales Lakehouse - Run Pipeline"

# Remover as tarefas
Unregister-ScheduledTask -TaskName "ERP Sales Lakehouse - Simulate Daily Activity" -Confirm:$false
Unregister-ScheduledTask -TaskName "ERP Sales Lakehouse - Run Pipeline" -Confirm:$false
```

## Bugs reais encontrados montando isso (vale registrar)

Rodar o pipeline de verdade contra dados "de agora" (em vez do seed
histórico original, espalhado em 18 meses no passado) expôs três problemas
que nenhum teste anterior tinha pego:

1. **Predicado incremental quebrava no SQL Server 2025**: a versão anterior
   de `src/extract.read_incremental_table` reconstruía um `DATETIME`
   truncado via `DATEADD(SECOND, DATEDIFF(...), âncora)` e comparava contra
   uma string — o otimizador dessa versão do SQL Server (2025 RTM-GDR)
   disparava "conversão de varchar para datetime fora do intervalo" de
   forma intermitente. Corrigido comparando os dois lados como **inteiro**
   (segundos desde a âncora), sem reconstruir data nenhuma — mais simples
   e mais robusto (ver comentário em `src/extract.py`).

2. **Timezone**: o SQL Server grava `UpdatedAt`/`IssueDate` via
   `SYSUTCDATETIME()` (UTC "sem fuso"). Sem configuração explícita, o Spark
   nesta máquina (fuso `E. South America Standard Time`, UTC-3) interpretava
   esses valores como hora LOCAL, deslocando-os 3h para frente — o
   suficiente para notas recém-criadas parecerem ter data de emissão no
   futuro e caírem na quarentena da Silver por engano. Isso só apareceu
   agora porque o seed original (datas de meses atrás) nunca chegou perto
   o bastante do "agora" para expor o problema. Corrigido forçando UTC na
   sessão Spark inteira (`spark.sql.session.timeZone` + `-Duser.timezone`
   na JVM, ver `src/utils.get_spark_session`). Como o bug já tinha
   **gravado** valores errados na Bronze antes da correção, foi necessário
   apagar `data/lakehouse/` e reprocessar do zero — a lição: um bug de
   timezone não se corrige só mudando o código, o dado já persistido
   errado também precisa de um backfill.

3. **`run_pipeline.ps1` (orquestração, não Spark/SQL)**: redirecionar stderr
   de um executável nativo com o operador `*>>`/`2>&1` do PowerShell 5.1
   faz cada linha de log normal do Spark (INFO/WARN, que vai para stderr)
   virar um "erro fatal" do PowerShell — corrigido usando redirecionamento
   nativo via `cmd /c "... >> log 2>&1"`. Também havia uma corrida de
   arquivo (log momentaneamente travado, provavelmente por antivírus
   escaneando o arquivo recém-escrito) — corrigido com retry na escrita do
   log — e uma janela de tempo entre o fim de uma JVM do Spark e o início
   da próxima que ocasionalmente causava falha de conexão — corrigido com
   uma folga de 5s entre etapas (exceto após a última, onde não há próxima
   JVM esperando).

4. **Mojibake nas tabelas `erp.*` de origem** (não é bug do pipeline em si,
   mas foi descoberto olhando o dado real no Power BI): `sql/02_insert_sample_data.sql`
   e `sql/05_simulate_daily_activity.sql` têm texto acentuado literal (ex.:
   "São Paulo", "Divergência no pedido"). Rodar esses arquivos .sql (UTF-8)
   via `sqlcmd` **sem** especificar a code page de entrada faz o sqlcmd ler o
   arquivo com a code page padrão do console — cada caractere acentuado (2
   bytes em UTF-8) vira 2 caracteres Latin-1 separados no banco ("São" virava
   "SÃ£o"). Como `sql/05_simulate_daily_activity.sql` roda TODO DIA via Task
   Scheduler, esse bug corromperia dado novo continuamente, não só o seed
   histórico. Corrigido em duas frentes: (a) `scripts/run_daily_erp_simulation.ps1`
   agora invoca `sqlcmd -f 65001` (UTF-8), então a simulação diária não
   corrompe mais nada daqui em diante; (b) o histórico já corrompido foi
   corrigido diretamente nas tabelas de origem por
   `scripts/fix_source_encoding.py` (script de execução única, reverte o
   double-encoding e atualiza `UpdatedAt` de cada linha corrigida para que a
   extração incremental da Bronze propague a correção sozinha, sem precisar
   reprocessar do zero).

Nenhum desses apareceria só escrevendo o código e rodando uma vez com o
seed histórico — só apareceram rodando de verdade, repetidamente, com
dados "de hoje", exatamente o cenário que a automação existe para cobrir.
