# Power BI — ERP Sales Lakehouse

Este diretório contém o material para montar o `.pbix` do dashboard executivo
a partir da camada Diamond. O `.pbix` em si ainda não foi criado — este
documento é o roteiro para montá-lo no Power BI Desktop, o mais acionável e
pré-validado possível dado que este ambiente não tem acesso ao Power BI
Desktop em si (sem automação de GUI, nada aqui pôde ser conferido
visualmente — tudo que É verificável programaticamente foi verificado, ver
seção "O que foi validado").

Conteúdo deste diretório:

- `publish_to_sql.py` — publica a Diamond no banco `ERP_Sales_BI` (SQL Server
  local), fonte de dados recomendada hoje (ver "Caminho 1 — SQL Server
  local"). É o último passo de `scripts/run_pipeline.ps1`.
- `export_snapshot.py` — gera um snapshot Parquet alternativo (ver "Caminho
  1-B — Parquet local"), mantido no repositório mas **fora** do pipeline
  agendado desde que o Caminho 1 passou a existir.
- `export/` — saída do script acima (Parquet, **gerado, não versionado** —
  está no `.gitignore`; regenerar rodando `export_snapshot.py` manualmente,
  se optar por esse caminho).
- Este `README.md` — conexão, modelo semântico, medidas DAX, plano de páginas.

## Princípio geral

**Estrutura fica no Lakehouse, análise fica no DAX.** Nenhuma tabela Diamond
deveria precisar de uma coluna calculada ou medida estrutural no Power BI
(receita, margem, ticket médio, ranking global, atingimento de meta já vêm
prontos — ver `docs/business_rules.md` e o cabeçalho de
`notebooks/04_create_diamond.py`). Medidas DAX no `.pbix` devem se limitar a
lógica genuinamente dependente do contexto de filtro do relatório: comparação
entre períodos escolhidos pelo usuário, ranking que reage a um slicer,
formatação de exibição. A lista completa está na seção "Medidas DAX".

---

## Como conectar (três caminhos)

O projeto roda hoje em `RUN_MODE=local` (decisão tomada: sem migração para
AWS por ora). Isso importa para a conexão do Power BI porque **Power BI
Desktop standalone não tem conector nativo para pasta Delta Lake em disco**
(isso existe no Fabric/Synapse Lakehouse, não no Power BI Desktop puro).

### Por que não apontar direto para `./data/lakehouse/diamond/<tabela>/`

`write_diamond()` (em `notebooks/04_create_diamond.py`) grava cada tabela
Diamond com `.mode("overwrite")`. O Delta Lake, ao sobrescrever, marca os
arquivos parquet antigos como removidos no log de transação
(`_delta_log/*.json`) mas **não os apaga fisicamente** — isso só acontece com
`VACUUM`, que nenhuma rotina deste projeto chama. Um leitor que entende o log
(Spark+Delta, Athena) sabe ignorar os arquivos removidos; o conector de
Parquet/pasta do Power BI não entende `_delta_log` e leria **todos** os
arquivos `.parquet` fisicamente presentes na pasta, ignorando quais foram
"substituídos".

Isso não é um risco teórico. Prova rodada nesta sessão (2026-07-15): a pasta
local `data/lakehouse/diamond/commercial_kpis/` tinha 3 arquivos
`part-*.parquet` remanescentes de execuções anteriores do notebook 04:

```text
spark.read.format("delta").load(path).count()  -> 19 linhas  (CORRETO)
spark.read.parquet(path).count()                -> 57 linhas  (19 x 3, ERRADO)
```

Ou seja: apontar o Power BI direto para a pasta Delta local triplicaria os
KPIs comerciais nesta rodada — e o número exato de duplicação varia a cada
vez que o pipeline roda de novo, o que tornaria o erro difícil de notar (o
dashboard "funciona", só está errado).

### Caminho 1 — SQL Server local (hoje, recomendado)

**`powerbi/publish_to_sql.py`** lê cada uma das 6 tabelas Diamond pelo motor
Delta (respeitando `_delta_log`, portanto sempre a versão atual e correta) e
publica cada uma como uma tabela relacional em **`ERP_Sales_BI`** — um banco
SQL Server dedicado, separado do banco de origem do ERP (`ERP_Sales`), criado
por `sql/06_create_bi_database.sql`. É o último passo de
`scripts/run_pipeline.ps1`, rodando automaticamente todo dia (ver
`docs/automation.md`).

Por que um banco separado (e não escrever de volta em `ERP_Sales`): mistura
o papel de "sistema fonte" (o ERP transacional simulado) com o de "sistema de
consumo analítico" — numa migração futura pra AWS essas duas coisas são
fisicamente diferentes (SQL Server on-prem vs. Athena/Glue). O login que
escreve (`erp_bi_writer`) só tem permissão em `ERP_Sales_BI`, sem acesso
nenhum ao schema `erp.*` de origem — mesmo princípio de least privilege do
login de leitura (`erp_extractor`), aplicado no sentido inverso.

**Passo a passo:**

1. Pré-requisito (uma vez só, já feito nesta máquina): criar o banco e o
   login de escrita —

   ```powershell
   sqlcmd -S localhost,14333 -E -C -v LoginPassword="SuaSenhaForte" -i sql\06_create_bi_database.sql
   ```

   e preencher `SQLSERVER_BI_DATABASE`/`SQLSERVER_BI_USER`/`SQLSERVER_BI_PASSWORD`
   no `.env` (ver `.env.example`).

2. Publicar (ou republicar) a Diamond no banco BI:

   ```bash
   .venv/Scripts/python.exe powerbi/publish_to_sql.py
   ```

   Isso sobrescreve as 6 tabelas em `ERP_Sales_BI.dbo.*`: `monthly_sales`,
   `product_ranking`, `customer_ranking`, `salesperson_performance`,
   `target_vs_actual`, `commercial_kpis`.

3. No Power BI Desktop: **Obter Dados → Mais... → Banco de Dados → SQL
   Server**.

4. Servidor: `localhost,14333` (ou o host/porta reais, ver `.env`). Banco de
   dados: `ERP_Sales_BI`. Modo de conectividade de dados: **Importar**.

5. Autenticação: **Banco de dados**, usuário `erp_bi_writer` (ou crie um
   login de leitura dedicado só para o Power BI se preferir não reutilizar o
   de escrita — mesma lógica de least privilege, ver observação abaixo).

6. No navegador, selecione as 6 tabelas em `dbo` e carregue.

7. Configure relacionamentos (ver seção "Modelo semântico"), crie as medidas
   (ver "Medidas DAX") e monte as páginas (ver "Plano de páginas").

> **Sobre reusar `erp_bi_writer` para leitura no Power BI:** aceitável neste
> projeto de portfólio (o banco só contém dado já agregado/público dentro do
> próprio pipeline, sem PII sensível adicional), mas o padrão mais correto em
> produção seria um terceiro login **SELECT-only** em `ERP_Sales_BI`,
> dedicado ao Power BI — nem o de extração (leitura da origem) nem o de
> escrita (publicação da Diamond) deveriam também ser o de consumo do BI.
> Não implementado aqui para não multiplicar logins num projeto local de
> demonstração, mas vale a nota para quem for levar este padrão a produção.

**Atualização (refresh):** ver seção "Atualização automática" abaixo — este é
o caminho que finalmente permite agendamento de verdade sem depender de AWS,
via Power BI Service + Gateway de Dados Local.

### Caminho 1-B — Parquet local (alternativa mais simples, sem banco)

Mantido no repositório (`powerbi/export_snapshot.py`) para quem preferir não
configurar banco/gateway — útil para uma conferência rápida dos números sem
montar a infraestrutura do Caminho 1. Deixou de ser o passo padrão do
pipeline agendado (`scripts/run_pipeline.ps1` roda `publish_to_sql.py`, não
mais `export_snapshot.py`).

Resolve o mesmo problema do Caminho 1 (não apontar direto pra pasta Delta):
lê cada tabela pelo motor Delta e regrava como **um único arquivo Parquet
solto** por tabela em `powerbi/export/<tabela>.parquet`. Formato Parquet (não
CSV) porque carrega o schema tipado no próprio arquivo — sem depender da
inferência de tipo do Power Query, sensível à localidade regional do Windows.

Passo a passo resumido: `.venv/Scripts/python.exe powerbi/export_snapshot.py`
→ Power BI Desktop → **Obter Dados → Mais... → Arquivo → Parquet** → aponte
para **um arquivo por vez** em `powerbi/export/` (nunca "Obter Dados → Pasta"
— o conector de pasta combina os 6 schemas diferentes num dataset sem
sentido) → repita para as 6 tabelas → **Import**.

**Atualização (refresh):** manual, sempre — sem gateway/agendamento possível
(arquivo local, fora do alcance do Power BI Service sem um gateway apontando
pra essa pasta especificamente). Fluxo: `export_snapshot.py` → no Power BI
Desktop, **Página Inicial → Atualizar**.

### Caminho 2 — Produção (Athena, quando `RUN_MODE=aws` for aplicado)

**Não testável agora** (não migramos para AWS) — documentado para quando a
migração acontecer, já que a infraestrutura já está definida em
`infra/terraform/glue.tf`/`athena.tf`.

1. **Pré-requisito de infraestrutura** (já provisionado no Terraform, não é
   trabalho novo): `aws_glue_crawler.lakehouse["diamond"]` usa `delta_target`
   apontando para cada prefixo `s3://<bucket-diamond>/<tabela>/` — isso
   popula o Glue Data Catalog automaticamente, sem exigir manifest Delta
   (`create_native_delta_table = true`). As 6 tabelas Diamond aparecem no
   catálogo com o **mesmo nome de pasta**, sem prefixo `diamond_`:
   `monthly_sales`, `product_ranking`, `customer_ranking`,
   `salesperson_performance`, `target_vs_actual`, `commercial_kpis` — dentro
   do database `erp_sales_lakehouse` (valor de `GLUE_DATABASE_NAME`
   no `.env` / `var.glue_database_name` no Terraform), junto com as tabelas
   de bronze/silver/gold (distinguíveis pelo padrão de nome: bronze usa
   prefixo `erp_`, silver nomes simples, gold usa `dim_`/`fact_`).

2. Instalar o **driver ODBC Simba Athena** (Amazon Athena ODBC Driver) na
   máquina Windows onde o Power BI Desktop roda.

3. Configurar um DSN apontando para:
   - Workgroup: `${var.project_name}-${var.environment}` (ex.:
     `erp-sales-lakehouse-dev`, ver `infra/terraform/athena.tf`);
   - S3 Output Location: bucket de resultados do Athena
     (`aws_s3_bucket.athena_results`, ver `infra/terraform/s3.tf`);
   - Região: `var.aws_region` (default `us-east-1`);
   - Credenciais AWS (chave/secret ou perfil).

4. No Power BI Desktop: **Obter Dados → Mais... → Banco de Dados → Amazon
   Athena**. Selecione o DSN configurado (ou preencha servidor/porta
   manualmente).

5. No navegador, selecione o database `erp_sales_lakehouse` e as 6 tabelas
   Diamond.

6. Modo de armazenamento: **Import**, não DirectQuery — mesmo raciocínio do
   caminho local: a Diamond já é pré-agregada; usar DirectQuery faria o Power
   BI disparar uma query Athena a cada interação do relatório (custo +
   latência), o que anula o motivo de a Diamond existir (processar uma vez
   no Lakehouse, não recalcular/reconsultar a cada abertura do relatório).

7. Relacionamentos, medidas e páginas: idênticos ao caminho local, **exceto**
   pela seção de relacionamentos (ver abaixo) — no Athena, as tabelas
   `gold.dim_*` completas também estão disponíveis no catálogo, então o
   modelo em estrela completo (Diamond relacionada às dimensões da Gold) é
   possível; no snapshot local isso não é o caso.

### Atualização automática (o motivo de tudo isso existir)

O ERP de origem muda todo dia (novas notas, devoluções, metas). Pra o
dashboard acompanhar isso sozinho, duas peças precisam estar agendadas —
**nenhuma delas depende de alguém abrir o Power BI Desktop**. Isso já é
possível **hoje, sem AWS**, usando o Caminho 1 (SQL Server local).

**a) Agendar o pipeline de dados — já implementado localmente:**

Duas tarefas no Windows Task Scheduler (`docs/automation.md`): uma simula
atividade nova no ERP, a outra roda `scripts/run_pipeline.ps1`
(`Bronze → Silver → Gold → Diamond → publish_to_sql.py`) 15 minutos depois.
Sem custo, sem depender de nenhum serviço cloud. Em produção (quando migrar
pra AWS), o equivalente seria um **Databricks Job** com 4 tasks em sequência
e agendamento *cron* — ver observação ao final desta seção.

**b) Agendar a atualização no Power BI Service** (depois que o pipeline do
dia terminar) — dois sub-caminhos, dependendo de qual fonte o `.pbix` usa:

**Se a fonte for o Caminho 1 (SQL Server local, `ERP_Sales_BI`) — automático
sem AWS:**

1. Instalar o **Gateway de Dados Local (modo pessoal)** nesta máquina —
   gratuito, é o mesmo mecanismo que qualquer empresa usa para o Power BI
   Service enxergar um SQL Server on-premises. Download e instruções:
   [Gateway de dados local — Microsoft Learn](https://learn.microsoft.com/power-bi/connect-data/service-gateway-onprem).
2. Publicar o `.pbix` no Power BI Service (`Arquivo → Publicar`).
3. No workspace, nas configurações do dataset: associar o Gateway instalado
   e configurar **Atualização agendada → Ativado**.
4. Frequência: diária, horário definido **depois** do horário da tarefa
   `ERP Sales Lakehouse - Run Pipeline` (hoje às 05:15 — ver
   `docs/automation.md`), com margem de segurança (ex.: refresh às 06:00).
5. Credenciais do SQL Server (`erp_bi_writer` ou um login de leitura
   dedicado, ver nota no Caminho 1) são configuradas uma vez no dataset
   publicado; o Power BI Service as reutiliza em cada atualização agendada
   através do Gateway.

**Se a fonte for Athena (Caminho 2, produção/AWS):** não precisa de Gateway
— Athena é um serviço cloud nativo, o Power BI Service acessa direto. Mesmos
passos 2-4 acima, pulando o passo 1.

**Se a fonte for o Caminho 1-B (Parquet local):** não há agendamento real
possível sem também instalar um Gateway apontando pra essa pasta — e mesmo
assim seria mais frágil que o Caminho 1 (seção de risco de leitura direta de
pasta Delta não se aplica ao Parquet solto, mas o arquivo ainda depende de
alguém rodar `export_snapshot.py` manualmente antes do refresh). Por isso
não é o caminho recomendado para automação — use o Caminho 1.

**Opção mais avançada (acoplamento mais forte, não implementada aqui):** em
vez de confiar só no horário fixo do passo 4, o próprio job de pipeline pode
disparar a atualização do Power BI ao final da execução, chamando a
[Power BI REST API](https://learn.microsoft.com/rest/api/power-bi/datasets/refresh-dataset)
(`POST /datasets/{id}/refreshes`) como último passo — o dashboard atualiza
assim que o dado está pronto, em vez de esperar uma janela de tempo fixa que
pode ser cedo demais (pipeline atrasou) ou tarde demais (dado pronto há uma
hora, mas ninguém viu ainda). Funciona tanto com o Task Scheduler local
quanto com um Databricks Job em produção — é uma chamada HTTP no fim do
script/task, independente de quem orquestra.

---

## Tabelas Diamond disponíveis

| Tabela | Grão | Linhas (validado nesta sessão) | Uso sugerido no relatório |
| --- | --- | --- | --- |
| `monthly_sales` | mês × região (do vendedor) | 140 | tendência de receita/margem/ticket médio, com slicer de região |
| `product_ranking` | 1 linha por produto (período completo) | 24 | Top produtos por receita, margem, quantidade ou devolução |
| `customer_ranking` | 1 linha por cliente (período completo) | 20 | Top clientes por receita ou devolução |
| `salesperson_performance` | 1 linha por vendedor (período completo) | 10 | performance e devolução por vendedor |
| `target_vs_actual` | vendedor × região × ano × mês | 186 | meta vs. realizado, atingimento por vendedor/mês |
| `commercial_kpis` | 1 linha por mês (empresa) | 19 | cards de topo (receita, margem, ticket médio, atingimento de meta) |

Contagens conferem com `docs/data_dictionary.md` (seção Diamond) e com a
leitura Delta original — ver "O que foi validado" ao final.

Dicionário completo de colunas: `docs/data_dictionary.md` (seção Diamond).
Os tipos exatos publicados (conferidos nesta sessão lendo de volta tanto as
tabelas em `ERP_Sales_BI` quanto os `.parquet` gerados) estão documentados
inline nos comentários de `powerbi/publish_to_sql.py`/`powerbi/export_snapshot.py`
e reproduzidos abaixo apenas onde relevante para uma medida DAX específica.

---

## Modelo semântico — relacionamentos

### Caminho 1 / 1-B — local (SQL Server ou Parquet)

**Nenhum relacionamento entre as 6 tabelas Diamond é necessário nem
recomendado nos caminhos locais.** Isso é uma revisão em relação à primeira
versão deste documento: a recomendação anterior de relacionar
`monthly_sales`/`target_vs_actual` às dimensões `gold.dim_*` (por
`region_key`/`salesperson_key`) só faz sentido se essas dimensões também
estiverem carregadas no Power BI — e **não estão**: tanto
`publish_to_sql.py` quanto `export_snapshot.py` publicam só as 6 tabelas
Diamond, não `gold.dim_*` (fora do escopo da camada Diamond). As colunas
`*_key` continuam presentes (ex.: `monthly_sales.region_key`,
`target_vs_actual.salesperson_key`) para o dia em que alguém decidir também
publicar as dimensões da Gold, mas hoje elas não têm a que se relacionar.

Cada tabela Diamond já é autocontida (atributos descritivos — nome, região,
categoria, segmento — denormalizados) e funciona como tabela standalone,
alimentando os visuais da sua própria página. Isso é, na prática, o cenário
mais simples possível: 6 tabelas independentes, sem risco de relacionamento
muitos-para-muitos porque não há relacionamento nenhum.

**Se, ao montar o relatório, for necessário um slicer que filtre visuais de
páginas diferentes pela mesma região** (ex.: selecionar "Sudeste" e afetar
tanto o gráfico de `monthly_sales` quanto a tabela de `target_vs_actual`),
isso não é possível sem uma dimensão compartilhada. Duas opções, em ordem de
preferência:

1. Resolver no Lakehouse: se esse cruzamento vier a ser um requisito real do
   dashboard, o caminho correto é adicionar `dim_region` (e as demais `dim_*`
   necessárias) à lista de tabelas exportadas por `export_snapshot.py` — não
   recriar a dimensão no Power BI. Mantém o princípio "estrutura no
   Lakehouse".
2. Se for só para o snapshot local e não vale o esforço agora: montar, via
   Power Query, uma "dimensão-ponte" mínima (nova consulta em branco,
   `Table.Distinct` sobre a coluna `region_name` combinada das tabelas que a
   têm) — é um padrão aceitável quando não há dimensão real disponível, mas
   deve ser tratado como solução temporária, documentada no próprio `.pbix`
   (não versionada em código Python, porque não existe no Lakehouse).

Nenhuma das duas foi necessária para o plano de páginas atual (seção
abaixo) — cada página usa slicers dentro da(s) própria(s) tabela(s) que
carrega.

### Caminho Athena/produção (futuro)

Nesse cenário `gold.dim_*` está disponível no mesmo catálogo, então a
recomendação original volta a valer: relacionar `monthly_sales` e
`target_vs_actual` às `dim_region`/`dim_salesperson` completas por
`region_key`/`salesperson_key` (lado "um" nas `dim_*`, lado "muitos" na
Diamond) permite filtrar o relatório por atributos que não foram
denormalizados na Diamond (ex.: `dim_salesperson.is_active`). Sem risco de
muitos-para-muitos porque cada tabela Diamond tem no máximo 1 linha por
combinação de chaves + período. `commercial_kpis` continua sem chaves de
dimensão (grão só por `year_month`) — não deve ser relacionada a nada, é a
tabela de cards de topo.

---

## Convenção de exclusão de notas canceladas

Todas as tabelas Diamond já excluem `invoice_status = 'Cancelada'` (e os 3
itens órfãos da nota quarentenada `invoice_id = 500`) — ver
`docs/business_rules.md` e o cabeçalho de `notebooks/04_create_diamond.py`
para a decisão completa. O Power BI não precisa (e não deve) reaplicar esse
filtro — já está embutido nos números agregados.

---

## Medidas DAX

Todas as medidas abaixo evitam `SAMEPERIODLASTYEAR`/`DATEADD` de propósito:
essas funções de time intelligence exigem uma tabela de calendário real,
contínua e marcada como "Tabela de Datas" — que dependeria de
`gold.dim_date`, não exportada no caminho local (só as 6 tabelas Diamond
são). Em vez disso, as medidas usam aritmética direta sobre as colunas
`year`/`month` (ou `target_year`/`target_month`) que cada tabela já carrega,
o que funciona igual nos dois caminhos de conexão (local ou Athena) sem
depender de uma dimensão de data adicional.

### 1. Comparação mês a mês / ano a ano — `commercial_kpis`

```dax
Receita Líquida =
SUM ( commercial_kpis[receita_liquida] )
```

```dax
Receita Líquida (Mês Anterior) =
VAR AnoAtual = SELECTEDVALUE ( commercial_kpis[year] )
VAR MesAtual = SELECTEDVALUE ( commercial_kpis[month] )
VAR IndiceAtual = AnoAtual * 12 + MesAtual
VAR IndiceAnterior = IndiceAtual - 1
VAR AnoAnterior = INT ( ( IndiceAnterior - 1 ) / 12 )
VAR MesAnterior = IndiceAnterior - AnoAnterior * 12
RETURN
    CALCULATE (
        SUM ( commercial_kpis[receita_liquida] ),
        ALL ( commercial_kpis ),
        commercial_kpis[year] = AnoAnterior,
        commercial_kpis[month] = MesAnterior
    )
```

```dax
Var Receita Líquida MoM % =
DIVIDE (
    [Receita Líquida] - [Receita Líquida (Mês Anterior)],
    [Receita Líquida (Mês Anterior)]
)
```

```dax
Receita Líquida (Mesmo Mês Ano Anterior) =
VAR AnoAtual = SELECTEDVALUE ( commercial_kpis[year] )
VAR MesAtual = SELECTEDVALUE ( commercial_kpis[month] )
RETURN
    CALCULATE (
        SUM ( commercial_kpis[receita_liquida] ),
        ALL ( commercial_kpis ),
        commercial_kpis[year] = AnoAtual - 1,
        commercial_kpis[month] = MesAtual
    )
```

```dax
Var Receita Líquida YoY % =
DIVIDE (
    [Receita Líquida] - [Receita Líquida (Mesmo Mês Ano Anterior)],
    [Receita Líquida (Mesmo Mês Ano Anterior)]
)
```

Notas de uso:

- `SELECTEDVALUE` retorna `BLANK()` se mais de um mês estiver selecionado no
  contexto — essas medidas são para cards de KPI filtrados a **um** mês por
  vez (via slicer de `year_month`), não para o gráfico de tendência (que deve
  mostrar a série inteira, sem esse filtro — ver "Plano de páginas").
- A mesma lógica se aplica a `monthly_sales`, trocando `ALL(commercial_kpis)`
  por `ALL(monthly_sales[year_month], monthly_sales[year], monthly_sales[month], monthly_sales[month_name])`
  (removendo `ALL` apenas das colunas de tempo, preservando o filtro de
  `region_key`/`region_name` — assim a comparação MoM continua "dentro da
  mesma região" quando o relatório estiver filtrado por região).

### 2. Ranking dinâmico (`RANKX`) — `product_ranking` / `customer_ranking` / `salesperson_performance`

As colunas `rank_receita_liquida` etc. já existentes nessas tabelas são
**ranks globais** (calculados uma vez na Diamond, sobre todo o período/todo o
universo de produtos/clientes/vendedores) — corretas para um "Top N" fixo,
mas não mudam se o usuário aplicar um slicer (ex.: por `category_name`). Para
um rank que reage ao filtro do relatório:

```dax
Rank Receita Líquida (Dinâmico) =
RANKX (
    ALLSELECTED ( product_ranking ),
    CALCULATE ( SUM ( product_ranking[receita_liquida] ) ),
    ,
    DESC,
    DENSE
)
```

Mesmo padrão para as outras duas tabelas:

```dax
Rank Receita Líquida Cliente (Dinâmico) =
RANKX (
    ALLSELECTED ( customer_ranking ),
    CALCULATE ( SUM ( customer_ranking[receita_liquida] ) ),
    ,
    DESC,
    DENSE
)
```

```dax
Rank Receita Líquida Vendedor (Dinâmico) =
RANKX (
    ALLSELECTED ( salesperson_performance ),
    CALCULATE ( SUM ( salesperson_performance[receita_liquida] ) ),
    ,
    DESC,
    DENSE
)
```

`ALLSELECTED` (não `ALL`) é o que faz o rank reagir a slicers de página/
relatório mas ainda respeitar filtros de visual (contexto padrão de uma
matriz/tabela) — é a função certa para "rank dentro do que está filtrado
agora", diferente de `ALL` (ignoraria os slicers também) ou de não usar
nenhuma (rankearia só as linhas visíveis no visual, quebrando com
paginação/Top N do visual).

### 3. Variação de `percentual_atingimento_meta` entre períodos — `target_vs_actual`

Atenção a uma pegadinha real de agregação: a coluna
`target_vs_actual[percentual_atingimento_meta]` é uma **razão pré-calculada
por linha** (grão vendedor × região × mês). Arrastar essa coluna para um
cartão/visual com múltiplas linhas no contexto e agregar por `AVERAGE` está
**errado** — média de percentuais não é o atingimento combinado (ex.: média
simples de 50% e 150% dá 100%, que não é necessariamente o atingimento real
do grupo, que depende do peso de cada meta). A forma correta é recalcular a
razão em cima das somas:

```dax
% Atingimento Meta =
DIVIDE (
    SUM ( target_vs_actual[receita_liquida_realizada] ),
    SUM ( target_vs_actual[target_value] )
)
```

```dax
% Atingimento Meta (Mês Anterior) =
VAR AnoAtual = SELECTEDVALUE ( target_vs_actual[target_year] )
VAR MesAtual = SELECTEDVALUE ( target_vs_actual[target_month] )
VAR IndiceAtual = AnoAtual * 12 + MesAtual
VAR IndiceAnterior = IndiceAtual - 1
VAR AnoAnterior = INT ( ( IndiceAnterior - 1 ) / 12 )
VAR MesAnterior = IndiceAnterior - AnoAnterior * 12
RETURN
    CALCULATE (
        DIVIDE (
            SUM ( target_vs_actual[receita_liquida_realizada] ),
            SUM ( target_vs_actual[target_value] )
        ),
        ALL ( target_vs_actual[target_year], target_vs_actual[target_month] ),
        target_vs_actual[target_year] = AnoAnterior,
        target_vs_actual[target_month] = MesAnterior
    )
```

```dax
Var % Atingimento Meta (p.p.) =
[% Atingimento Meta] - [% Atingimento Meta (Mês Anterior)]
```

`Var % Atingimento Meta (p.p.)` é **subtração direta**, não `DIVIDE` — a
diferença entre dois percentuais se expressa em pontos percentuais (p.p.),
não em variação percentual da variação (que seria uma segunda camada de
razão, confusa de mais para esse indicador).

### 4. Formatação de KPIs — moeda BRL

**Forma recomendada (padrão, use esta):** definir o formato **na própria
medida**, não com uma medida de texto. Em `[Receita Líquida]` → aba
"Medida"/"Ferramentas de Medida" → **Formato → Moeda → R$ Português
(Brasil)** (ou formato customizado `"R$" #,##0.00;-"R$" #,##0.00`). Isso
mantém a medida numérica (funciona em cartão, gráfico, matriz, ordenação,
formatação condicional) — uma medida que retorna texto formatado
(`FORMAT(...)`) quebra tudo isso e só deve ser usada quando o visual exige
literalmente uma string (caso abaixo).

**Caso específico que exige texto — cartão combinando valor + variação**,
para um cartão de KPI customizado (visual de texto, não o cartão nativo):

```dax
Receita Líquida (Cartão com Variação) =
VAR ReceitaAtual = [Receita Líquida]
VAR Variacao = [Var Receita Líquida MoM %]
VAR Seta = IF ( Variacao >= 0, "▲", "▼" )
RETURN
    FORMAT ( ReceitaAtual, "R$ #,##0.00" ) & "  " & Seta & " " & FORMAT ( Variacao, "0.0%" )
```

Use esta medida **só** no visual de texto/cartão customizado que precisa da
combinação valor+seta numa única string — nunca em vez da medida numérica
`[Receita Líquida]` nos demais visuais (cartão nativo, gráfico de tendência,
tabela), onde a formatação via propriedade "Formato" é sempre preferível.

---

## Plano de páginas do dashboard

Quatro páginas, cada uma alimentada por um subconjunto claro de tabelas
Diamond — evita uma página "genérica" que tentaria cruzar tabelas sem
relacionamento (ver seção de relacionamentos: nenhuma existe no caminho
local hoje).

### Página 1 — Visão Executiva

**Fonte:** `commercial_kpis`.

- Cartões de topo: `[Receita Líquida]` (formato moeda), `margem_percentual`
  (agregado — usar `AVERAGE` é aceitável aqui porque é 1 linha por mês, sem
  múltiplas linhas no contexto do cartão quando filtrado a um mês),
  `ticket_medio`, `[% Atingimento Meta]` (reaproveitando a mesma lógica
  soma/soma da seção de medidas, adaptada para `commercial_kpis` se preferir
  não reusar `target_vs_actual` nesta página).
- Cartões de variação: `[Var Receita Líquida MoM %]`, `[Var Receita Líquida YoY %]`.
- Slicer: `year_month` (controla os cartões acima).
- Gráfico de linha: `receita_liquida` e `margem_valor` por `year_month`
  (série completa, tendência) — **usar "Editar Interações"
  (Formatar → Editar Interações) para que o slicer de `year_month` NÃO afete
  este gráfico** (marcar como "Nenhum"), senão o gráfico de tendência vira um
  gráfico de um ponto só toda vez que alguém filtra o mês para ver os
  cartões. Esse é o motivo de existir tanto o cartão (contexto de 1 mês)
  quanto o gráfico (contexto de todos os meses) na mesma página.

### Página 2 — Vendas e Metas

**Fonte:** `target_vs_actual` (+ `monthly_sales` opcionalmente, para dar
contexto de tendência regional na mesma página).

- Slicers: `target_year`, `target_month`, `region_name`.
- Velocímetro (gauge): `[% Atingimento Meta]`.
- Cartão: `[Var % Atingimento Meta (p.p.)]`.
- Gráfico de barras (meta vs. realizado) por vendedor: `target_value` e
  `receita_liquida_realizada`, eixo = `salesperson_name`.
- Tabela detalhada: `salesperson_name`, `region_name`, `target_value`,
  `receita_liquida_realizada`, `percentual_atingimento_meta` (aqui, coluna
  original é aceitável — 1 linha por vendedor/mês, sem agregação ambígua),
  `tem_meta_cadastrada` (para destacar vendedores sem meta cadastrada, que
  vêm com `percentual_atingimento_meta` nulo por desenho — ver decisão de
  modelagem no cabeçalho de `04_create_diamond.py`).

### Página 3 — Rankings

**Fonte:** `product_ranking`, `customer_ranking`, `salesperson_performance`
(três visuais lado a lado ou três abas/bookmarks).

- Slicers: `category_name` (produtos), `customer_segment` (clientes),
  `region_name` (vendedores) — cada slicer afeta só a tabela correspondente
  (sem relacionamento entre elas, o que é esperado neste layout).
- Gráficos de barras horizontais "Top N" (usar o filtro visual nativo "Top N"
  do Power BI sobre `receita_liquida` — não recriar em DAX, é estrutural e já
  suportado pelo próprio visual).
- Tabelas detalhadas com as colunas `rank_*` estáticas (rank global, sempre
  visível) **e** as medidas `Rank Receita Líquida (Dinâmico)` /
  equivalentes lado a lado — para o usuário perceber a diferença entre
  "top do ano todo" (coluna) e "top dentro do que estou filtrando agora"
  (medida).

### Página 4 — Devoluções

**Fonte:** `commercial_kpis` (tendência mensal) + `product_ranking` /
`customer_ranking` / `salesperson_performance` (quebra por entidade).

- Cartões: `valor_devolvido` (soma mensal, de `commercial_kpis`),
  `percentual_devolucao` (nível empresa).
- Gráfico de linha: `valor_devolvido` por `year_month` (mesmo padrão de
  "Editar Interações" da Página 1, se houver slicer de mês na página).
- Gráficos de barras Top 10 por `valor_devolvido`: um para produtos, um para
  clientes, um para vendedores (usa as colunas `rank_valor_devolvido` já
  prontas em cada tabela de ranking, sem recalcular).

---

## O que foi validado (e o que não pôde ser)

Validado programaticamente nesta sessão (sem Power BI Desktop disponível):

- `powerbi/publish_to_sql.py` rodado de ponta a ponta, tanto isolado quanto
  dentro de `scripts/run_pipeline.ps1` (via Task Scheduler): as 6 tabelas
  publicadas em `ERP_Sales_BI` batem em contagem de linhas com a leitura
  Delta original (`monthly_sales`=143, `product_ranking`=24,
  `customer_ranking`=20, `salesperson_performance`=10, `target_vs_actual`=186,
  `commercial_kpis`=19), e a contagem foi conferida de forma independente
  via `sqlcmd` diretamente contra `ERP_Sales_BI` (não só confiando no
  autocheck do próprio script).
- `powerbi/export_snapshot.py` (Caminho 1-B) também rodado de ponta a ponta:
  as 6 tabelas exportadas batem em contagem de linhas E em lista de colunas
  com a leitura Delta original.
- Reproduzido o bug de leitura direta da pasta Delta (`commercial_kpis`: 19
  linhas via Delta vs. 57 via parquet cru), confirmando por que nenhum dos
  dois caminhos locais lê a pasta Delta diretamente.
- Schema/tipos de cada `.parquet` exportado foram lidos de volta e conferidos
  (tipos `decimal`/`double`/`int`/`bigint`/`boolean`/`string` preservados,
  sem inferência de texto envolvida).

**Não pôde ser testado** (sem acesso a Power BI Desktop/automação de GUI
neste ambiente): a importação real das tabelas/arquivos no Power BI, o
comportamento visual das medidas DAX, o layout final das páginas, o Gateway
de Dados Local (instalação/configuração é uma ação na conta pessoal do
usuário no Power BI Service), e o caminho Athena (depende de migração para
`RUN_MODE=aws`, fora do escopo desta sessão). As medidas DAX seguem
padrões-safe e amplamente documentados
(`SELECTEDVALUE`/`CALCULATE`/`ALL`/`RANKX`/`ALLSELECTED`/`DIVIDE`), mas devem
ser conferidas visualmente pelo usuário ao montar o `.pbix` — se algum
resultado não bater, o primeiro lugar a checar é o filtro de contexto
(`SELECTEDVALUE` retorna `BLANK()` se mais de um valor estiver selecionado).
