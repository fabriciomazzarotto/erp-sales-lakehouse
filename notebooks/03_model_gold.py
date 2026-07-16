# Databricks notebook source
"""
03_model_gold.py

Modelagem dimensional (star schema) a partir da Silver: dimensões com
surrogate key própria da Gold (dim_customer, dim_product, dim_salesperson,
dim_payment_method, dim_region, dim_date) e fatos (fact_sales, fact_returns,
fact_sales_targets) com as métricas de negócio calculadas de forma
padronizada (fonte da verdade: docs/business_rules.md).

Por que star schema: a Silver já entrega dados limpos e deduplicados, mas no
grão transacional bruto da origem (chave técnica *ID do ERP, sem otimização
para consumo analítico). A Gold reestrutura isso em dimensões conformadas
(reutilizáveis por qualquer fato) e fatos finos, o que permite ao Power BI
(via Diamond) fazer joins simples por surrogate key inteira (mais rápido que
join por string/composto) e mantém a lógica de métricas centralizada em um
único lugar — evitando que Diamond/Power BI reimplementem as fórmulas de
receita/margem cada um à sua maneira.

Por que surrogate key própria (e não o *ID técnico da origem): o *ID técnico
é um IDENTITY do SQL Server, controlado pela origem — a Gold não deve
depender do seu ciclo de vida (ex.: se a origem um dia trocar de sistema,
reciclar IDs, ou se duas origens forem unificadas, IDs técnicos podem colidir
ou ficar obsoletos). A Gold gera sua própria chave, estável dentro do seu
próprio domínio, e preserva o *_id de origem como atributo de rastreabilidade
(nunca é descartado, só deixa de ser a chave "pública" do modelo).

Geração determinística da surrogate key: row_number() sobre a chave de
origem ordenada (não monotonically_increasing_id(), que não é estável entre
execuções/reparticionamentos). Como a Gold recalcula tudo do zero a cada
rodada (overwrite, mesmo padrão da Silver), isso garante idempotência: rodar
duas vezes seguidas com a mesma Silver produz exatamente as mesmas surrogate
keys.

--------------------------------------------------------------------------
DECISÃO DE MODELAGEM — desconto de nota (cabeçalho) vs. desconto de item
--------------------------------------------------------------------------
sales_invoice_header.discount_value é um desconto ÚNICO por NOTA; o grão do
fact_sales é por ITEM de nota. Três opções foram consideradas:

1) Repetir o desconto total da nota em cada linha de item: quebra a
   aditividade — somar valor_desconto dos itens de uma nota com N itens
   contaria o desconto da nota N vezes (super-estimando o desconto real).
2) Ignorar o desconto de cabeçalho no grão de item (só aplicar o desconto do
   item): mantém a aditividade, mas subestima sistematicamente a receita
   líquida por item sempre que a nota tiver desconto de cabeçalho > 0 —
   viola a fórmula de negócio (receita_liquida deixaria de refletir
   valor_desconto real da venda).
3) Ratear o desconto do cabeçalho entre os itens da nota, proporcional à
   receita bruta de cada item dentro da nota (escolhida):

       desconto_rateado_item = discount_value_header
                                * (receita_bruta_item / receita_bruta_nota)

   valor_desconto (no fact_sales) = discount_value do item + desconto_rateado_item

Trade-off assumido: o rateio proporcional por receita é uma aproximação — a
origem não registra COMO o desconto de cabeçalho foi de fato distribuído
entre os itens (pode ter sido um valor fixo aplicado a um item específico,
por exemplo). A vantagem decisiva é preservar a aditividade: SUM(valor_desconto)
dos itens de uma nota sempre bate com o discount_value do cabeçalho + a soma
dos descontos de item, o que é essencial para qualquer agregação por nota,
cliente, produto ou período feita depois na Diamond. Quando invoice_gross_revenue
da nota é 0 (todos os itens com unit_price = 0 — caso extremo, não observado
nos dados atuais), o rateio não é aplicável e o desconto de cabeçalho fica em 0
nessas linhas (documentado, não travarrocessamento).

--------------------------------------------------------------------------
DECISÃO DE MODELAGEM — region_key em fact_sales
--------------------------------------------------------------------------
sales_invoice_header não tem RegionID direto (só Customers e Salespersons
têm). region_key em fact_sales foi definido como a região do VENDEDOR
(salesperson), não do cliente, porque:
- silver.sales_targets já é modelada por (salesperson_id, region_id) — ou
  seja, a meta comercial é acompanhada pelo território do vendedor. Usar a
  mesma semântica de região em fact_sales permite comparar realizado vs. meta
  por região sem ambiguidade na camada Diamond.
- Região do vendedor = território de venda (semântica de "quota"); região do
  cliente = geografia de demanda. São perguntas de negócio diferentes. Se no
  futuro for necessário analisar por região do CLIENTE, isso já está
  disponível via dim_customer.region_name (join por customer_key) sem
  necessidade de reprocessar o fact.

--------------------------------------------------------------------------
DECISÃO DE MODELAGEM — status da nota (invoice_status) em fact_sales
--------------------------------------------------------------------------
Todos os itens de nota entram em fact_sales, independentemente de
invoice_status ('Emitida'/'Cancelada'). O status é mantido como atributo
degenerado (não é uma dimensão própria — só 2 valores, baixa cardinalidade,
não precisa de chave substituta) para que a camada Diamond decida se inclui
ou exclui notas canceladas das métricas executivas. Isso também é o motivo
de fact_sales ter exatamente 1302 linhas (mesmo grão/contagem de
silver.sales_invoice_items) — não há filtro de negócio aplicado aqui.

--------------------------------------------------------------------------
LIMITAÇÃO CONHECIDA — 3 itens órfãos de cabeçalho (não é bug da Gold)
--------------------------------------------------------------------------
A validação de FK da Silver para sales_invoice_items (02_transform_silver.py)
checa InvoiceID contra a Bronze CRUA de cabeçalho (para garantir que a nota
existe estruturalmente na origem), não contra a Silver de cabeçalho já
validada. Isso significa que os 3 itens da nota InvoiceID=500 (a única nota
com IssueDate futura, que foi para quarentena na Silver) permanecem em
silver.sales_invoice_items normalmente. Ao montar fact_sales com LEFT JOIN
para silver.sales_invoice_header (necessário para preservar o grão de 1302
itens), essas 3 linhas ficam com customer_key/salesperson_key/
payment_method_key/region_key/date_key/invoice_status nulos — não há como
resolver essas dimensões porque a nota-mãe não existe na Silver validada.
Está documentado e validado explicitamente em validate_gold() abaixo (nenhum
outro item deve ter chave nula).
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.config import get_table_path
from src.utils import get_logger, get_spark_session

logger = get_logger("model_gold")

# COMMAND ----------
# Helpers
# COMMAND ----------


def read_silver(spark, table):
    return spark.read.format("delta").load(get_table_path("silver", table))


def write_gold(df, table_name):
    path = get_table_path("gold", table_name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    count = df.count()
    logger.info(f"[gold.{table_name}] gravado — {count} linhas")
    return count


def add_surrogate_key(df, key_name, order_column):
    """
    Gera a surrogate key própria da Gold: row_number() determinístico sobre a
    chave de origem ordenada. Ver nota de topo do módulo sobre por que não
    usamos monotonically_increasing_id() aqui (não é estável entre execuções).
    """
    window = Window.orderBy(order_column)
    return df.withColumn(key_name, F.row_number().over(window))


# COMMAND ----------
# Dimensões
# COMMAND ----------


def build_dim_region(spark):
    regions = read_silver(spark, "regions")
    dim = add_surrogate_key(regions, "region_key", "region_id")
    dim = dim.select("region_key", "region_id", "region_code", "region_name", "state", "country")
    write_gold(dim, "dim_region")
    return dim


def build_dim_payment_method(spark):
    payment_methods = read_silver(spark, "payment_methods")
    dim = add_surrogate_key(payment_methods, "payment_method_key", "payment_method_id")
    dim = dim.select(
        "payment_method_key",
        "payment_method_id",
        "payment_method_code",
        "payment_method_name",
        "payment_type",
    )
    write_gold(dim, "dim_payment_method")
    return dim


def build_dim_customer(spark, regions_silver):
    customers = read_silver(spark, "customers")
    dim = customers.join(
        regions_silver.select("region_id", "region_name"), "region_id", "left"
    )
    dim = add_surrogate_key(dim, "customer_key", "customer_id")
    dim = dim.select(
        "customer_key",
        "customer_id",
        "customer_code",
        "customer_name",
        "document",
        "email",
        "phone",
        "customer_segment",
        "region_id",
        "region_name",
        "is_active",
    )
    write_gold(dim, "dim_customer")
    return dim


def build_dim_product(spark):
    products = read_silver(spark, "products")
    dim = add_surrogate_key(products, "product_key", "product_id")
    dim = dim.select(
        "product_key",
        "product_id",
        "product_code",
        "product_name",
        "category_name",
        "unit_of_measure",
        "unit_cost",
        "unit_price",
        "is_active",
    )
    write_gold(dim, "dim_product")
    return dim


def build_dim_salesperson(spark, regions_silver):
    salespersons = read_silver(spark, "salespersons")
    dim = salespersons.join(
        regions_silver.select("region_id", "region_name"), "region_id", "left"
    )
    dim = add_surrogate_key(dim, "salesperson_key", "salesperson_id")
    dim = dim.select(
        "salesperson_key",
        "salesperson_id",
        "salesperson_code",
        "salesperson_name",
        "region_id",
        "region_name",
        "hire_date",
        "is_active",
    )
    write_gold(dim, "dim_salesperson")
    return dim


def build_dim_date(spark, header, returns, targets):
    """
    dim_date não vem de uma tabela Silver — é gerada programaticamente
    cobrindo o período dos dados reais: issue_date (vendas), return_date
    (devoluções) e target_year/target_month (metas, que podem cobrir meses
    sem nenhuma venda ainda registrada). O range é expandido para o
    primeiro e o último dia dos meses de borda, garantindo meses completos
    (útil para agregações mensais na Diamond mesmo perto das bordas do
    período coberto).
    """
    dates_from_header = header.select(F.to_date("issue_date").alias("d"))
    dates_from_returns = returns.select(F.to_date("return_date").alias("d"))
    dates_from_targets = targets.select(
        F.to_date(
            F.format_string("%04d-%02d-01", F.col("target_year"), F.col("target_month"))
        ).alias("d")
    )

    all_dates = dates_from_header.union(dates_from_returns).union(dates_from_targets)
    bounds = all_dates.agg(F.min("d").alias("min_d"), F.max("d").alias("max_d")).collect()[0]

    range_start = bounds["min_d"].replace(day=1)
    if bounds["max_d"].month == 12:
        next_month_first = bounds["max_d"].replace(year=bounds["max_d"].year + 1, month=1, day=1)
    else:
        next_month_first = bounds["max_d"].replace(month=bounds["max_d"].month + 1, day=1)
    range_end = next_month_first - __import__("datetime").timedelta(days=1)

    logger.info(f"[gold.dim_date] cobrindo {range_start} até {range_end}")

    date_df = spark.sql(
        f"SELECT explode(sequence(to_date('{range_start}'), to_date('{range_end}'), interval 1 day)) AS full_date"
    )

    dim = (
        date_df.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))  # 1=domingo .. 7=sábado (padrão Spark)
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
        .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_of_week",
            "day_name",
            "week_of_year",
            "is_weekend",
            "year_month",
        )
    )
    write_gold(dim, "dim_date")
    return dim


# COMMAND ----------
# Fatos
# COMMAND ----------


def build_fact_sales(spark, dim_customer, dim_product, dim_salesperson, dim_payment_method, dim_region):
    header = read_silver(spark, "sales_invoice_header")
    items = read_silver(spark, "sales_invoice_items")
    returns = read_silver(spark, "sales_returns")

    # Devoluções agregadas por item de nota (mesmo grão do fact_sales) —
    # fonte única do componente valor_devolucao da fórmula de receita líquida.
    returns_agg = returns.groupBy("invoice_item_id").agg(
        F.sum(F.col("quantity") * F.col("unit_value")).alias("valor_devolucao"),
        F.sum("quantity").alias("quantidade_devolvida"),
    )

    items_gross = items.withColumn("item_gross_revenue", F.col("quantity") * F.col("unit_price")).withColumnRenamed(
        "discount_value", "item_discount_value"
    )
    invoice_gross = items_gross.groupBy("invoice_id").agg(
        F.sum("item_gross_revenue").alias("invoice_gross_revenue")
    )

    header_slim = header.select(
        "invoice_id",
        "invoice_number",
        "invoice_series",
        "customer_id",
        "salesperson_id",
        "payment_method_id",
        "issue_date",
        "invoice_status",
        F.col("discount_value").alias("header_discount_value"),
    )

    base = (
        items_gross.join(invoice_gross, "invoice_id", "left")
        .join(header_slim, "invoice_id", "left")  # LEFT: preserva grão de 1302 itens (ver nota de topo)
        .join(returns_agg, "invoice_item_id", "left")
    )

    base = base.withColumn(
        "header_discount_allocated",
        F.when(
            F.col("invoice_gross_revenue") > 0,
            F.col("header_discount_value") * (F.col("item_gross_revenue") / F.col("invoice_gross_revenue")),
        ).otherwise(F.lit(0.0)),
    )
    base = base.fillna({"valor_devolucao": 0.0, "quantidade_devolvida": 0.0, "header_discount_allocated": 0.0})

    base = base.withColumn("receita_bruta", F.col("item_gross_revenue"))
    base = base.withColumn("valor_desconto", F.col("item_discount_value") + F.col("header_discount_allocated"))
    base = base.withColumn(
        "receita_liquida", F.col("receita_bruta") - F.col("valor_desconto") - F.col("valor_devolucao")
    )

    base = base.join(
        dim_product.select("product_id", "product_key", F.col("unit_cost").alias("_product_unit_cost")),
        "product_id",
        "left",
    )
    base = base.withColumn("custo_total", F.col("quantity") * F.col("_product_unit_cost"))
    base = base.withColumn("margem_valor", F.col("receita_liquida") - F.col("custo_total"))
    base = base.withColumn(
        "margem_percentual",
        F.when(F.col("receita_liquida") != 0, F.col("margem_valor") / F.col("receita_liquida")).otherwise(
            F.lit(None).cast("double")
        ),
    )

    base = base.join(dim_customer.select("customer_id", "customer_key"), "customer_id", "left")
    base = base.join(
        dim_salesperson.select(
            "salesperson_id", "salesperson_key", F.col("region_id").alias("_salesperson_region_id")
        ),
        "salesperson_id",
        "left",
    )
    base = base.join(dim_payment_method.select("payment_method_id", "payment_method_key"), "payment_method_id", "left")
    base = base.join(
        dim_region.select(F.col("region_id").alias("_salesperson_region_id"), "region_key"),
        "_salesperson_region_id",
        "left",
    )
    base = base.withColumn("date_key", F.date_format("issue_date", "yyyyMMdd").cast("int"))

    base = add_surrogate_key(base, "sales_key", "invoice_item_id")

    fact = base.select(
        "sales_key",
        "invoice_item_id",
        "invoice_id",
        "invoice_number",
        "invoice_series",
        "item_sequence",
        "customer_key",
        "product_key",
        "salesperson_key",
        "payment_method_key",
        "region_key",
        "date_key",
        "invoice_status",
        "quantity",
        "unit_price",
        "item_discount_value",
        "header_discount_allocated",
        "valor_desconto",
        "valor_devolucao",
        "quantidade_devolvida",
        "receita_bruta",
        "receita_liquida",
        "custo_total",
        "margem_valor",
        "margem_percentual",
    )
    write_gold(fact, "fact_sales")
    return fact


def build_fact_returns(spark, dim_customer, dim_product, dim_salesperson, dim_payment_method, dim_region):
    returns = read_silver(spark, "sales_returns")
    header = read_silver(spark, "sales_invoice_header")

    header_slim = header.select(
        "invoice_id", "salesperson_id", "payment_method_id", F.col("issue_date").alias("_header_issue_date")
    )

    base = returns.join(header_slim, "invoice_id", "left")  # LEFT: preserva grão de 95 devoluções

    base = base.withColumn("valor_devolvido", F.col("quantity") * F.col("unit_value"))

    base = base.join(dim_customer.select("customer_id", "customer_key"), "customer_id", "left")
    base = base.join(dim_product.select("product_id", "product_key"), "product_id", "left")
    base = base.join(
        dim_salesperson.select(
            "salesperson_id", "salesperson_key", F.col("region_id").alias("_salesperson_region_id")
        ),
        "salesperson_id",
        "left",
    )
    base = base.join(dim_payment_method.select("payment_method_id", "payment_method_key"), "payment_method_id", "left")
    base = base.join(
        dim_region.select(F.col("region_id").alias("_salesperson_region_id"), "region_key"),
        "_salesperson_region_id",
        "left",
    )
    base = base.withColumn("date_key", F.date_format("return_date", "yyyyMMdd").cast("int"))

    base = add_surrogate_key(base, "return_key", "return_id")

    fact = base.select(
        "return_key",
        "return_id",
        "return_number",
        "invoice_id",
        "invoice_item_id",
        "customer_key",
        "product_key",
        "salesperson_key",
        "payment_method_key",
        "region_key",
        "date_key",
        "quantity",
        "unit_value",
        "valor_devolvido",
        "return_reason",
    )
    write_gold(fact, "fact_returns")
    return fact


def build_fact_sales_targets(spark, dim_salesperson, dim_region):
    targets = read_silver(spark, "sales_targets")

    base = targets.withColumn(
        "_target_month_first_day",
        F.to_date(F.format_string("%04d-%02d-01", F.col("target_year"), F.col("target_month"))),
    )
    base = base.withColumn("date_key", F.date_format("_target_month_first_day", "yyyyMMdd").cast("int"))

    base = base.join(dim_salesperson.select("salesperson_id", "salesperson_key"), "salesperson_id", "left")
    base = base.join(dim_region.select("region_id", "region_key"), "region_id", "left")

    base = add_surrogate_key(base, "target_key", "target_id")

    fact = base.select(
        "target_key",
        "target_id",
        "salesperson_key",
        "region_key",
        "target_year",
        "target_month",
        "date_key",
        "target_value",
    )
    write_gold(fact, "fact_sales_targets")
    return fact


# COMMAND ----------
# Validação (sanity checks contra a Silver)
# COMMAND ----------


def validate_gold(spark, dims, facts):
    logger.info("== Validação Gold ==")
    ok = True

    expected_counts = {
        "dim_region": read_silver(spark, "regions").count(),
        "dim_payment_method": read_silver(spark, "payment_methods").count(),
        "dim_customer": read_silver(spark, "customers").count(),
        "dim_product": read_silver(spark, "products").count(),
        "dim_salesperson": read_silver(spark, "salespersons").count(),
        "fact_sales": read_silver(spark, "sales_invoice_items").count(),
        "fact_returns": read_silver(spark, "sales_returns").count(),
        "fact_sales_targets": read_silver(spark, "sales_targets").count(),
    }
    all_tables = {**dims, **facts}
    for table_name, expected in expected_counts.items():
        actual = all_tables[table_name].count()
        status = "OK" if actual == expected else "MISMATCH"
        if actual != expected:
            ok = False
        logger.info(f"[contagem] {table_name}: silver={expected} gold={actual} -> {status}")

    # Órfãos de dimensão: surrogate key nula em fact_sales, EXCETO as 3 linhas
    # conhecidas da nota InvoiceID=500 (quarantinada na Silver — ver nota de
    # topo do módulo). Qualquer órfão além dessas 3 linhas indica bug real.
    fact_sales = facts["fact_sales"]
    known_orphan_invoice_id = 500
    unexpected_orphans = fact_sales.filter(
        (F.col("customer_key").isNull() | F.col("salesperson_key").isNull() | F.col("payment_method_key").isNull()
         | F.col("region_key").isNull() | F.col("date_key").isNull())
        & (F.col("invoice_id") != known_orphan_invoice_id)
    ).count()
    known_orphans = fact_sales.filter(
        (F.col("customer_key").isNull() | F.col("salesperson_key").isNull() | F.col("payment_method_key").isNull()
         | F.col("region_key").isNull() | F.col("date_key").isNull())
        & (F.col("invoice_id") == known_orphan_invoice_id)
    ).count()
    logger.info(
        f"[órfãos fact_sales] esperados (invoice_id={known_orphan_invoice_id}, cabeçalho quarentenado)={known_orphans}, "
        f"inesperados={unexpected_orphans}"
    )
    if unexpected_orphans > 0:
        ok = False

    # Órfãos de dimensão em fact_returns — não deveria haver nenhum (produto e
    # cliente vêm direto da própria linha de devolução, sempre validados na
    # Silver; vendedor/região vêm de um join com header cujo invoice_id é o
    # mesmo já referenciado pela devolução, sempre presente na Silver).
    fact_returns = facts["fact_returns"]
    returns_orphans = fact_returns.filter(
        F.col("customer_key").isNull() | F.col("product_key").isNull() | F.col("salesperson_key").isNull()
        | F.col("payment_method_key").isNull() | F.col("region_key").isNull() | F.col("date_key").isNull()
    ).count()
    logger.info(f"[órfãos fact_returns] inesperados={returns_orphans}")
    if returns_orphans > 0:
        ok = False

    # fact_sales_targets — não deveria haver nenhum órfão (salesperson_id e
    # region_id são FK validadas na própria Silver de sales_targets).
    fact_targets = facts["fact_sales_targets"]
    targets_orphans = fact_targets.filter(
        F.col("salesperson_key").isNull() | F.col("region_key").isNull() | F.col("date_key").isNull()
    ).count()
    logger.info(f"[órfãos fact_sales_targets] inesperados={targets_orphans}")
    if targets_orphans > 0:
        ok = False

    # dim_date: todo date_key usado pelos fatos deve existir na dim_date.
    dim_date = dims["dim_date"]
    dim_date_keys = dim_date.select("date_key")
    for fact_name in ["fact_sales", "fact_returns", "fact_sales_targets"]:
        used_keys = facts[fact_name].select("date_key").filter(F.col("date_key").isNotNull()).distinct()
        missing = used_keys.join(dim_date_keys, "date_key", "left_anti").count()
        logger.info(f"[dim_date] date_keys de {fact_name} não encontrados em dim_date: {missing}")
        if missing > 0:
            ok = False

    logger.info(f"== Validação Gold: {'PASSOU' if ok else 'FALHOU'} ==")
    return ok


# COMMAND ----------


def main():
    spark = get_spark_session("erp-model-gold")
    try:
        regions_silver = read_silver(spark, "regions")
        header_silver = read_silver(spark, "sales_invoice_header")
        returns_silver = read_silver(spark, "sales_returns")
        targets_silver = read_silver(spark, "sales_targets")

        dim_region = build_dim_region(spark)
        dim_payment_method = build_dim_payment_method(spark)
        dim_customer = build_dim_customer(spark, regions_silver)
        dim_product = build_dim_product(spark)
        dim_salesperson = build_dim_salesperson(spark, regions_silver)
        dim_date = build_dim_date(spark, header_silver, returns_silver, targets_silver)

        fact_sales = build_fact_sales(spark, dim_customer, dim_product, dim_salesperson, dim_payment_method, dim_region)
        fact_returns = build_fact_returns(spark, dim_customer, dim_product, dim_salesperson, dim_payment_method, dim_region)
        fact_sales_targets = build_fact_sales_targets(spark, dim_salesperson, dim_region)

        dims = {
            "dim_region": dim_region,
            "dim_payment_method": dim_payment_method,
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_salesperson": dim_salesperson,
            "dim_date": dim_date,
        }
        facts = {
            "fact_sales": fact_sales,
            "fact_returns": fact_returns,
            "fact_sales_targets": fact_sales_targets,
        }
        validate_gold(spark, dims, facts)
    finally:
        spark.stop()

    logger.info("Modelagem Gold finalizada")


if __name__ == "__main__":
    main()
