# Regras de negócio — ERP Sales Lakehouse

## Métricas principais

| Métrica | Fórmula | Onde é calculada |
|---|---|---|
| Receita bruta | `quantidade * valor_unitario` | `gold.fact_sales.receita_bruta` |
| Receita líquida | `receita_bruta - valor_desconto - valor_devolucao` | `gold.fact_sales.receita_liquida` |
| Ticket médio | `receita_liquida / quantidade_notas` | Diamond (agregação por período/vendedor/cliente) |
| Margem (valor) | `receita_liquida - custo_total` | `gold.fact_sales.margem_valor` |
| Margem (%) | `margem_valor / receita_liquida` | `gold.fact_sales.margem_percentual` |
| Atingimento de meta | `receita_liquida / valor_meta` | Diamond (cruza `gold.fact_sales` agregada com `gold.fact_sales_targets`) |

`custo_total = quantidade * unit_cost` (unit_cost de `Products.UnitCost`, ver `sql/01_create_tables.sql`, exposto em `gold.dim_product`).

A Gold calcula receita bruta/líquida, margem (valor e %) e os componentes de
devolução uma única vez, no grão de item de venda (`gold.fact_sales`); a
Diamond reaproveita essas colunas para agregações executivas (ticket médio,
atingimento de meta, rankings), em vez de reimplementar as fórmulas.

## Indicadores de devolução

- `valor_devolvido` (`quantidade * unit_value`), calculado em `gold.fact_returns`
  no grão de devolução, e como `valor_devolucao` agregado por item de venda
  em `gold.fact_sales` (mesma fórmula, grãos diferentes — ver
  `docs/data_dictionary.md`, seção Gold, para o detalhamento completo).
- `quantidade_devolvida`: exposta em `gold.fact_sales` (por item) e como
  `quantity` em `gold.fact_returns` (por devolução).
- `percentual_devolucao`: não calculado na Gold (é uma razão agregada,
  ex.: `SUM(valor_devolvido) / SUM(receita_bruta)` por produto/cliente/
  vendedor/período) — fica na Diamond, junto dos rankings de produtos/
  clientes/vendedores com maior devolução.

## Regras de qualidade (aplicadas na camada Silver)

- Cliente e produto não podem ter ID nulo
- Nota fiscal deve ter número, data e cliente
- Item de nota deve ter produto, quantidade e valor
- Quantidade vendida não pode ser zero ou negativa (exceto regras específicas de devolução)
- Valor unitário não pode ser negativo
- Data de emissão não pode ser futura

Ver `notebooks/02_transform_silver.py` para a implementação (via `src/quality.py`).

## Modelagem dimensional (camada Gold)

Ver `notebooks/03_model_gold.py` (implementação, com as decisões de
modelagem documentadas em comentários no topo do arquivo) e
`docs/data_dictionary.md` (seção Gold, com o dicionário completo de
dimensões e fatos). Duas decisões relevantes, resumidas aqui:

- **Desconto de cabeçalho vs. item**: `sales_invoice_header.discount_value`
  (por nota) é rateado proporcionalmente entre os itens da nota (por
  receita bruta), para preservar a aditividade da receita líquida somada
  por nota, sem perder o grão de item no `fact_sales`.
- **Região em `fact_sales`**: `region_key` reflete a região do vendedor
  (território de venda), consistente com a granularidade de
  `sales_targets` (por vendedor + região), permitindo comparar realizado
  vs. meta por região sem ambiguidade na Diamond.

## Agregados executivos (camada Diamond)

Ver `notebooks/04_create_diamond.py` (implementação e decisões de
modelagem documentadas em comentários no topo do arquivo) e
`docs/data_dictionary.md` (seção Diamond, com o dicionário completo das 6
tabelas). Decisão mais relevante, resumida aqui:

- **Notas `'Cancelada'` são excluídas de toda a Diamond.** `gold.fact_sales`
  mantém `invoice_status` sem filtro de propósito (decisão da Gold — ver
  acima), deixando para a Diamond decidir. A Diamond aplica
  `invoice_status = 'Emitida'` como filtro único e consistente em TODAS as
  agregações executivas (receita, margem, ticket médio, rankings, KPIs,
  meta vs. realizado) — uma nota cancelada não representa uma venda real.
  Isso também remove automaticamente os 3 itens órfãos da nota
  quarentenada `invoice_id = 500` (que ficam com `invoice_status` nulo,
  logo não são `'Emitida'`), sem necessidade de um filtro separado para
  esse caso.
- `ticket_medio = receita_liquida / quantidade_notas` é calculado sempre
  sobre `COUNT(DISTINCT invoice_id)` (nunca contagem de itens/linhas), para
  não inflar o denominador em notas com múltiplos itens.
- `percentual_devolucao = SUM(valor_devolvido) / SUM(receita_bruta)` é
  calculado por produto/cliente/vendedor em `product_ranking`,
  `customer_ranking` e `salesperson_performance`, a partir de
  `gold.fact_returns` (que não tem `invoice_status` — usada sem filtro
  adicional, ver `docs/data_dictionary.md` para a limitação assumida).
- `percentual_atingimento_meta = receita_liquida_realizada / target_value`
  é calculado em `diamond.target_vs_actual` (grão vendedor + região + ano +
  mês, via `FULL OUTER JOIN` entre meta e realizado) e replicado, já
  agregado por mês, em `diamond.commercial_kpis`.
- Validado contra a Gold: `SUM(receita_liquida)` das tabelas Diamond bate
  exatamente com `SUM(receita_liquida)` de `gold.fact_sales` filtrado por
  `invoice_status = 'Emitida'` (3.819.165,62); pipeline é idempotente
  (rodar duas vezes seguidas produz os mesmos dados, hash idêntico).
