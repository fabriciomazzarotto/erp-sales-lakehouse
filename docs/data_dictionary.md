# Dicionário de dados — ERP Sales Lakehouse

Documento completo: origem (SQL Server) e as 4 camadas do Lakehouse (Bronze/Silver/Gold/Diamond), todas implementadas e validadas rodando localmente (`RUN_MODE=local`).

## Origem — SQL Server (`ERP_Sales`, schema `erp`)

| Tabela | Descrição | Chave primária | Watermark (incremental) |
|---|---|---|---|
| `erp.Regions` | Regiões/territórios de venda | `RegionID` | `UpdatedAt` (carga full recomendada) |
| `erp.PaymentMethods` | Formas de pagamento | `PaymentMethodID` | `UpdatedAt` (carga full recomendada) |
| `erp.Customers` | Clientes | `CustomerID` | `UpdatedAt` |
| `erp.Products` | Produtos | `ProductID` | `UpdatedAt` |
| `erp.Salespersons` | Vendedores | `SalespersonID` | `UpdatedAt` |
| `erp.SalesInvoiceHeader` | Cabeçalho da nota fiscal de venda | `InvoiceID` | `UpdatedAt` |
| `erp.SalesInvoiceItems` | Itens da nota fiscal de venda | `InvoiceItemID` | `UpdatedAt` |
| `erp.SalesReturns` | Devoluções | `ReturnID` | `UpdatedAt` |
| `erp.SalesTargets` | Metas comerciais por vendedor/mês | `TargetID` | `UpdatedAt` |

Colunas e tipos completos: ver `sql/01_create_tables.sql` (fonte da verdade do schema de origem).

## Bronze — espelho da origem + controle técnico

Gerada por `notebooks/01_ingest_bronze.py` via JDBC (login dedicado `erp_extractor`, SELECT-only — ver `sql/04_create_pipeline_login.sql`). Fica em `./data/lakehouse/bronze/<tabela>/` em `RUN_MODE=local`. Sem regra de negócio aplicada — a Bronze preserva a origem o mais fielmente possível, inclusive os 3 registros propositalmente "sujos" do seed (`sql/02_insert_sample_data.sql`).

Toda tabela Bronze recebe 5 colunas técnicas de controle: `ingestion_timestamp`, `source_system` (`erp_sqlserver`), `source_table` (nome da tabela de origem, ex.: `erp.Customers`), `batch_id` (identificador único da execução), `load_type` (`full` ou `incremental`).

| Tabela Bronze | Estratégia | Chave primária | Linhas (validado) |
|---|---|---|---|
| `bronze.erp_regions` | full (overwrite) | `RegionID` | 8 |
| `bronze.erp_payment_methods` | full (overwrite) | `PaymentMethodID` | 5 |
| `bronze.erp_customers` | incremental (MERGE) | `CustomerID` | 20 |
| `bronze.erp_products` | incremental (MERGE) | `ProductID` | 24 |
| `bronze.erp_salespersons` | incremental (MERGE) | `SalespersonID` | 10 |
| `bronze.erp_sales_invoice_header` | incremental (MERGE) | `InvoiceID` | 500 |
| `bronze.erp_sales_invoice_items` | incremental (MERGE) | `InvoiceItemID` | 1304 |
| `bronze.erp_sales_returns` | incremental (MERGE) | `ReturnID` | 95 |
| `bronze.erp_sales_targets` | incremental (MERGE) | `TargetID` | 120 |

**Carga incremental (watermark em `UpdatedAt`):** `src/extract.read_incremental_table` filtra `UpdatedAt > último_valor_processado` via predicate pushdown (o corte é feito pelo SQL Server, não trazendo a tabela inteira para o Spark). `src/load.write_bronze_incremental` faz `MERGE` por chave primária, garantindo idempotência (reprocessar o mesmo batch não duplica).

**Pegadinha de precisão resolvida (documentada em `src/extract.py`):** `DATETIME2` do SQL Server guarda 7 dígitos decimais; o `TimestampType` do Spark só tem 6. Comparar direto causava reinclusão infinita de linhas (arredondamento em lados opostos da fronteira de microssegundo). Solução: truncar os dois lados para o segundo inteiro — suficiente para este domínio de negócio (não há necessidade de granularidade sub-segundo) e elimina a classe inteira do problema.

**Colunas e tipos completos por tabela:** idênticos à origem (`sql/01_create_tables.sql`) + as 5 colunas técnicas acima — nenhuma coluna é renomeada ou tipada de forma diferente na Bronze.

## Silver — dados limpos, padronizados e validados

Gerada por `notebooks/02_transform_silver.py` a partir da Bronze. Fica em `./data/lakehouse/silver/<tabela>/` em `RUN_MODE=local`, sempre `mode("overwrite")` (recalculada do zero a cada execução — diferente da Bronze, que é incremental). Granularidade transacional preservada (a Silver não agrega).

Padronizações aplicadas (`src/transformations.py`): nomes de coluna convertidos de PascalCase para snake_case (`CustomerID` → `customer_id`), colunas técnicas da Bronze removidas (`ingestion_timestamp`, `source_system`, `source_table`, `batch_id`, `load_type`).

Validações aplicadas (`src/quality.py`, pipeline composável — ver `tests/test_quality.py` para a cobertura unitária): cada `check_*` acumula motivo(s) de rejeição sem descartar a linha; `split_valid_rejected()` separa válidos de rejeitados no final, gravando os rejeitados em `<tabela>_quarantine` com a coluna `rejection_reason` (nunca descarte silencioso).

| Tabela Silver | Validações aplicadas | Válidos | Rejeitados (quarentena) |
|---|---|---|---|
| `silver.regions` | not-null (`region_id`, `region_code`), dedup por `region_id` | 8 | 0 |
| `silver.payment_methods` | not-null, dedup | 5 | 0 |
| `silver.customers` | not-null (id/code/nome), FK `region_id` → `regions`, dedup | 20 | 0 |
| `silver.products` | not-null, `unit_price` > 0, `unit_cost` >= 0, dedup | 24 | 0 |
| `silver.salespersons` | not-null, FK `region_id` → `regions`, dedup | 10 | 0 |
| `silver.sales_invoice_header` | not-null (id/número/cliente/data), data não pode ser futura, FK cliente/vendedor/forma de pagamento, nota deve ter >=1 item, dedup | 499 | **1** (`IssueDate_in_future`) |
| `silver.sales_invoice_items` | not-null, `quantity` > 0, `unit_price` >= 0, FK nota/produto, dedup | 1302 | **2** (`Quantity_zero_or_negative`, `UnitPrice_negative`) |
| `silver.sales_returns` | not-null, `quantity` > 0, FK nota/item/produto/cliente, dedup | 95 | 0 |
| `silver.sales_targets` | not-null, `target_value` > 0, FK vendedor/região, dedup | 120 | 0 |

Os 3 registros rejeitados são exatamente os 3 registros propositalmente "sujos" plantados no seed de dados (`sql/02_insert_sample_data.sql`) — validado rodando o pipeline de verdade contra o SQL Server local, não apenas em teste unitário.

**Limitação conhecida (documentada, não é bug):** a validação de FK de `sales_invoice_items` checa `invoice_id` contra a **Bronze crua** do cabeçalho (garantindo que a nota existe estruturalmente na origem), não contra a Silver de cabeçalho já validada. Por isso os 3 itens da nota `invoice_id=500` (quarentenada por data futura) permanecem em `silver.sales_invoice_items` normalmente — o efeito disso é tratado explicitamente na Gold (ver `fact_sales`, seção "Limitação conhecida" abaixo).

**Dedup:** `deduplicate_by_key` mantém a linha com maior `UpdatedAt` por chave primária — protege contra o caso de a origem gerar múltiplas versões da mesma linha antes de o Delta convergir.

## Gold — modelo dimensional (star schema)

Gerada por `notebooks/03_model_gold.py` a partir da Silver. Todas as tabelas
usam `mode("overwrite")` (full refresh a cada execução, mesmo padrão da
Silver — não incremental) e ficam em `./data/lakehouse/gold/<tabela>/` em
`RUN_MODE=local`.

Convenção de chave: toda dimensão tem uma **surrogate key própria**
(`*_key`, inteiro, gerada por `row_number()` determinístico sobre a chave de
origem — não `monotonically_increasing_id()`, que não é estável entre
execuções). A chave técnica de origem (`*_id`) é preservada como atributo de
rastreabilidade, mas não é mais a chave "pública" do modelo.

### Dimensões

| Tabela | Grão | Origem (Silver) | Chave |
|---|---|---|---|
| `gold.dim_region` | 1 linha por região | `silver.regions` | `region_key` |
| `gold.dim_payment_method` | 1 linha por forma de pagamento | `silver.payment_methods` | `payment_method_key` |
| `gold.dim_customer` | 1 linha por cliente | `silver.customers` + `region_name` (join `regions`) | `customer_key` |
| `gold.dim_product` | 1 linha por produto | `silver.products` | `product_key` |
| `gold.dim_salesperson` | 1 linha por vendedor | `silver.salespersons` + `region_name` (join `regions`) | `salesperson_key` |
| `gold.dim_date` | 1 linha por dia | Gerada programaticamente (não vem da Silver) | `date_key` (int `yyyyMMdd`) |

`gold.dim_date`: colunas `date_key, full_date, year, quarter, month,
month_name, day, day_of_week, day_name, week_of_year, is_weekend,
year_month`. O range coberto é calculado dinamicamente a partir do mínimo e
máximo entre `issue_date` (vendas), `return_date` (devoluções) e
`target_year`/`target_month` (metas — que podem cobrir meses sem nenhuma
venda ainda), expandido para cobrir meses completos (dia 1 ao último dia do
mês de borda). `month_name`/`day_name` seguem o locale padrão da JVM (inglês
nos ambientes testados).

`gold.dim_customer` e `gold.dim_salesperson` carregam `region_name` como
atributo descritivo denormalizado (não uma FK para `dim_region` dentro da
própria dimensão) — evita snowflake para uma consulta comum ("clientes por
região"), sem impedir o join formal via `region_id` se necessário.

### Fatos

| Tabela | Grão | Linhas (validado) | Origem (Silver) |
|---|---|---|---|
| `gold.fact_sales` | 1 linha por item de nota fiscal | 1302 | `silver.sales_invoice_items` + `silver.sales_invoice_header` + `silver.sales_returns` (agregada) |
| `gold.fact_returns` | 1 linha por devolução | 95 | `silver.sales_returns` + `silver.sales_invoice_header` (para salesperson/region) |
| `gold.fact_sales_targets` | 1 linha por (vendedor, ano, mês) | 120 | `silver.sales_targets` |

#### `gold.fact_sales`

Colunas de chave/degeneradas: `sales_key`, `invoice_item_id`, `invoice_id`,
`invoice_number`, `invoice_series`, `item_sequence`, `customer_key`,
`product_key`, `salesperson_key`, `payment_method_key`, `region_key`,
`date_key`, `invoice_status`.

Colunas de métrica (fórmulas — fonte da verdade: `docs/business_rules.md`):

| Coluna | Fórmula |
|---|---|
| `quantity`, `unit_price` | direto do item (silver) |
| `item_discount_value` | desconto lançado no próprio item (silver) |
| `header_discount_allocated` | desconto da NOTA rateado proporcionalmente à receita bruta do item — ver decisão de modelagem abaixo |
| `valor_desconto` | `item_discount_value + header_discount_allocated` |
| `valor_devolucao` | soma de `quantity * unit_value` em `silver.sales_returns` para este `invoice_item_id` |
| `quantidade_devolvida` | soma de `quantity` devolvida para este `invoice_item_id` (apoio a `percentual_devolucao`, calculado na Diamond) |
| `receita_bruta` | `quantity * unit_price` |
| `receita_liquida` | `receita_bruta - valor_desconto - valor_devolucao` |
| `custo_total` | `quantity * unit_cost` (unit_cost de `gold.dim_product`) |
| `margem_valor` | `receita_liquida - custo_total` |
| `margem_percentual` | `margem_valor / receita_liquida` (nulo quando `receita_liquida = 0`, para não gerar divisão por zero) |

**Decisão — região em `fact_sales`:** `region_key` reflete a região do
**vendedor** (não do cliente), porque `silver.sales_targets` já é modelada
por `(salesperson_id, region_id)` — usar a mesma semântica permite comparar
realizado vs. meta por região sem ambiguidade na Diamond. Região do cliente
continua disponível via `dim_customer.region_name`.

**Decisão — rateio do desconto de cabeçalho:** `sales_invoice_header.discount_value`
é um valor único por nota, mas o grão do fato é por item. Repetir o valor
total em cada item quebraria a aditividade (soma dos itens > desconto real
da nota); ignorá-lo subestimaria a receita líquida do item. Optou-se por
ratear proporcionalmente à receita bruta de cada item dentro da nota:

```text
header_discount_allocated = discount_value_header * (receita_bruta_item / receita_bruta_nota)
```

Validado: para as 98 notas com `discount_value > 0` na Silver, a soma de
`header_discount_allocated` por `invoice_id` bate com o `discount_value` do
cabeçalho (diferença ~1e-6, arredondamento de ponto flutuante). Trade-off
assumido: é uma aproximação — a origem não registra como o desconto de
cabeçalho foi de fato distribuído entre os itens.

**Limitação conhecida (não é bug):** os 3 itens da nota `invoice_id = 500`
(a única com `IssueDate` futura, quarentenada na Silver) permanecem em
`silver.sales_invoice_items` — a validação de FK da Silver para itens checa
contra a Bronze crua do cabeçalho, não contra a Silver já validada. Como o
`fact_sales` usa `LEFT JOIN` para preservar o grão completo de 1302 itens,
essas 3 linhas ficam com `customer_key`/`salesperson_key`/`payment_method_key`/
`region_key`/`date_key`/`invoice_status` nulos. É a única exceção esperada;
`03_model_gold.py::validate_gold()` confirma que não há nenhum outro órfão.

`invoice_status` ('Emitida'/'Cancelada') é mantido como atributo degenerado
em todas as 1302 linhas — nenhum filtro de negócio é aplicado na Gold; a
Diamond decide se inclui/exclui cancelamentos.

#### `gold.fact_returns`

Colunas: `return_key`, `return_id`, `return_number`, `invoice_id`,
`invoice_item_id` (rastreabilidade da venda original), `customer_key`,
`product_key` (direto da própria linha de devolução), `salesperson_key`,
`payment_method_key`, `region_key` (via join com `silver.sales_invoice_header`
por `invoice_id`, para permitir rankings de devolução por vendedor/região),
`date_key` (de `return_date`), `quantity`, `unit_value`, `valor_devolvido`
(`= quantity * unit_value`), `return_reason`.

#### `gold.fact_sales_targets`

Colunas: `target_key`, `target_id`, `salesperson_key`, `region_key`,
`target_year`, `target_month`, `date_key` (primeiro dia do mês da meta, para
permitir join com `dim_date`), `target_value`. Grão 1:1 com
`silver.sales_targets`, só troca as FKs de origem por surrogate keys.

### O que a Gold NÃO calcula (fica para a Diamond)

`ticket_medio`, `percentual_atingimento_meta`, `percentual_devolucao` e
rankings (produto/cliente/vendedor) são agregações que dependem de um nível
de granularidade (mensal, por vendedor, etc.) definido na camada de consumo
— ficam na Diamond, reaproveitando as métricas já calculadas na Gold em vez
de reimplementar as fórmulas de receita/margem.

## Diamond — agregados executivos para o Power BI

Gerada por `notebooks/04_create_diamond.py` a partir da Gold. Todas as
tabelas usam `mode("overwrite")` (full refresh a cada execução, mesmo padrão
de Silver/Gold) e ficam em `./data/lakehouse/diamond/<tabela>/` em
`RUN_MODE=local`. Nenhuma fórmula de receita/margem é reimplementada — todas
as tabelas reaproveitam as colunas já calculadas em `gold.fact_sales` /
`gold.fact_returns` (ver `docs/business_rules.md`), apenas agregando e
rankeando.

**Por que existe** (resumo — texto completo no topo do módulo): o medallion
clássico (Bronze/Silver/Gold) já entrega dado bruto → confiável → modelado
em star schema, suficiente para análise ad-hoc. Mas qualquer agregação feita
direto no Power BI (via DAX/Power Query) é recalculada a cada abertura do
relatório / mudança de filtro. A Diamond agrega uma única vez no Lakehouse
(a cada rodada do pipeline) e entrega tabelas já no grão de consumo —
processar uma vez é mais barato que recalcular a cada consulta no BI.

### Decisão — filtro de `invoice_status`

Todas as tabelas Diamond partem de uma base filtrada de `gold.fact_sales`
(`invoice_status = 'Emitida'`), aplicada de forma consistente em toda a
camada (função `sales_base()` em `04_create_diamond.py`):

- **Notas `'Cancelada'`** (63 das 1302 linhas de `fact_sales`) são
  **excluídas** de todas as agregações de receita/margem/ticket
  médio/rankings/KPIs/meta — uma nota cancelada não é uma venda real.
- **Os 3 itens órfãos** da nota `invoice_id = 500` (quarentenada na Silver
  por data futura, `invoice_status` nulo) já ficam de fora automaticamente
  pelo mesmo filtro (não são `'Emitida'`) — não precisam de tratamento
  separado.
- Resultado: **1236 linhas** (de 1302) entram nas agregações Diamond.

`gold.fact_returns` não tem `invoice_status` (tabela independente, grão de
devolução) — usada sem filtro adicional nos indicadores de devolução.
Limitação assumida e documentada no módulo: uma devolução vinculada a uma
nota cancelada (não observado nos dados atuais) entraria no numerador de
`percentual_devolucao` sem a nota cancelada correspondente no denominador.

### Tabelas

| Tabela | Grão | Linhas (validado) |
|---|---|---|
| `diamond.monthly_sales` | `(year_month, region_key)` — região do vendedor | 140 |
| `diamond.product_ranking` | 1 linha por produto (período completo) | 24 |
| `diamond.customer_ranking` | 1 linha por cliente (período completo) | 20 |
| `diamond.salesperson_performance` | 1 linha por vendedor (período completo) | 10 |
| `diamond.target_vs_actual` | `(salesperson_key, region_key, target_year, target_month)` | 186 |
| `diamond.commercial_kpis` | 1 linha por mês (`year_month`), visão consolidada de empresa | 19 |

#### `diamond.monthly_sales`

Colunas: `year_month`, `year`, `month`, `month_name` (denormalizadas de
`gold.dim_date`, para plotar série temporal sem precisar relacionar com uma
dimensão diária), `region_key`, `region_code`, `region_name`,
`receita_bruta`, `receita_liquida`, `valor_desconto`, `valor_devolucao`,
`margem_valor`, `margem_percentual`, `quantidade_vendida`,
`quantidade_notas` (**contagem distinta de `invoice_id`**, não de itens),
`ticket_medio` (`receita_liquida / quantidade_notas`).

Grão escolhido — mês + região do vendedor: fino o suficiente para dar
tendência mensal por região sem DAX, grosso o suficiente para não virar uma
cópia agregada do fato. Grão diário/por vendedor foi descartado (granular
demais para consumo executivo; disponível via `salesperson_performance` e
via `gold.fact_sales` diretamente, se necessário).

#### `diamond.product_ranking` / `diamond.customer_ranking` / `diamond.salesperson_performance`

Consolidam TODO o período disponível (não são cortadas por mês — o Power BI
usa slicers de data se precisar de um recorte, já que os campos de chave
permitem relacionar com `gold.fact_sales` se necessário). Atributos
descritivos (nome, categoria, segmento, região) são denormalizados das
respectivas `gold.dim_*` para a tabela funcionar sozinha em uma visual de
tabela/gráfico de barras, sem exigir join.

Colunas comuns: receita bruta/líquida, margem (valor e, em
`product_ranking`/`salesperson_performance`, percentual), quantidade
vendida, `quantidade_notas` (distinct `invoice_id`), indicadores de
devolução (`valor_devolvido`, `quantidade_devolvida`,
`percentual_devolucao = SUM(valor_devolvido) / SUM(receita_bruta)`, a partir
de `gold.fact_returns`, sem filtro de `invoice_status` — ver decisão acima).

Colunas de rank (pré-computadas com `dense_rank()`, ordenação descendente):
`rank_receita_liquida` (nas três tabelas), `rank_margem_valor` e
`rank_quantidade_vendida` (só em `product_ranking`), `rank_valor_devolvido`
(nas três tabelas). **Importante:** esses ranks são globais (calculados uma
vez sobre todo o período, sem filtro de contexto) — se o relatório precisar
de um rank que reaja a slicers de data/região interativamente, isso é lógica
genuinamente analítica e deve ser uma medida DAX (`RANKX`) no Power BI, não
recalculada aqui.

#### `diamond.target_vs_actual`

Cruza `gold.fact_sales_targets` com a receita líquida realizada (agregada de
`sales_base` por `salesperson_key + region_key + ano + mês`, via join com
`gold.dim_date`) usando **`FULL OUTER JOIN`**: preserva metas sem venda no
mês (`receita_liquida_realizada = 0`) e vendas sem meta cadastrada
(`percentual_atingimento_meta = NULL`, sinalizado por
`tem_meta_cadastrada = false` — nunca 0, para não confundir "sem meta" com
"meta não atingida"). `percentual_atingimento_meta = receita_liquida_realizada
/ target_value`.

#### `diamond.commercial_kpis`

Grão: 1 linha por `year_month`, nível de empresa (sem quebra por
região/vendedor — quebras já existem nas outras tabelas Diamond).
Colunas: `year_month`, `year`, `month`, `month_name`, `receita_bruta`,
`receita_liquida`, `margem_valor`, `margem_percentual`, `quantidade_vendida`,
`quantidade_notas`, `ticket_medio`, `quantidade_clientes_ativos`,
`quantidade_vendedores_ativos` (`countDistinct` de clientes/vendedores com
venda no mês), `valor_devolvido`, `percentual_devolucao`,
`valor_meta_total` (soma de `target_value` do mês, via
`diamond.target_vs_actual`, reaproveitado — não reagregado do zero),
`percentual_atingimento_meta` (`receita_liquida / valor_meta_total`, `NULL`
quando não há meta cadastrada para nenhum vendedor naquele mês — ver meses
2025-01 a 2025-07 nos dados atuais, que não têm meta cadastrada).

### Validação executada (`04_create_diamond.py::validate_diamond`)

Rodado contra a Gold real (`.venv/Scripts/python.exe notebooks/04_create_diamond.py`):

- `SUM(receita_liquida)` de `sales_base` (`fact_sales` filtrado por
  `invoice_status='Emitida'`) = **3.819.165,62** — bate exatamente (diff
  0.0000) com `SUM(receita_liquida)` de `monthly_sales`, `product_ranking`,
  `customer_ranking` e `salesperson_performance`.
- `SUM(quantidade_notas)` de `monthly_sales` = **478** = `COUNT(DISTINCT
  invoice_id)` de `sales_base` (uma nota pertence a exatamente 1
  vendedor/região/mês, então soma por grão bate com o distinct global).
- `commercial_kpis.receita_liquida` bate com `monthly_sales` agregado por
  `year_month` (sem quebra de região) em todos os 19 meses — 0 divergências.
- Nenhuma linha de `target_vs_actual` com meta cadastrada ficou sem
  `percentual_atingimento_meta` calculado.
- Conferência manual adicional (fora do script, ver sessão de validação):
  vendedor `VEND007` (Patrícia Gomes), março/2026 — receita líquida
  realizada manual (`SUM(receita_liquida)` filtrando `salesperson_key=7`,
  `invoice_status='Emitida'`, `date_key` no mês) = 25.869,380001; meta
  cadastrada = 24.740,00; `percentual_atingimento_meta` manual =
  1,0456499596200486 — idêntico ao valor gravado em `target_vs_actual`.
- **Idempotência confirmada**: pipeline rodado duas vezes seguidas; hash
  MD5 do conteúdo ordenado de cada uma das 6 tabelas Diamond foi idêntico
  entre as duas execuções (mesma contagem de linhas e mesmos valores).
