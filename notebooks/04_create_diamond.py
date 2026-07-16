# Databricks notebook source
"""
04_create_diamond.py

Camada Diamond: agregados executivos, pré-calculados a partir da Gold,
otimizados para consumo direto no Power BI (cards, gráficos de tendência,
rankings) sem recalcular métricas em Power Query/DAX a cada abertura do
relatório.

Por que a Diamond existe (trade-off "processar uma vez vs. recalcular
sempre"): Bronze/Silver/Gold já entregam dado bruto -> confiável -> modelado
em star schema. Isso é suficiente para análise ad-hoc, mas qualquer agregação
(receita por mês, ranking de produtos, atingimento de meta) feita direto no
Power BI via DAX é recalculada TODA VEZ que o relatório é aberto ou um filtro
muda, sobre o fact_sales inteiro (1302 linhas hoje, mas o padrão não escala:
em produção seriam milhões). A Diamond agrega uma única vez no Lakehouse (a
cada rodada do pipeline) e entrega tabelas já no grão de consumo — o Power BI
só precisa ler e exibir, não processar. O custo é secundário: pipeline mais
longo e a necessidade de re-rodar a Diamond sempre que a Gold mudar (mesmo
padrão de overwrite/full refresh já usado em todas as camadas).

Regra seguida em todo este módulo: nenhuma fórmula de receita/margem/desconto
é reimplementada aqui — todas as colunas de métrica já existem em
gold.fact_sales/gold.fact_returns (ver docs/business_rules.md). A Diamond
apenas filtra, agrega e rankeia.

--------------------------------------------------------------------------
DECISÃO DE MODELAGEM — notas 'Cancelada' e itens órfãos (invoice_id=500)
--------------------------------------------------------------------------
gold.fact_sales mantém TODAS as 1302 linhas (nenhum filtro de negócio foi
aplicado na Gold, de propósito — ver notebooks/03_model_gold.py). A Diamond é
a camada que decide:

1) invoice_status = 'Cancelada' (63 de 1302 linhas): EXCLUÍDO de todas as
   agregações executivas (receita, margem, ticket médio, rankings, KPIs,
   meta vs. realizado). Uma nota cancelada não é uma venda que de fato
   aconteceu — contá-la infla receita/quantidade/ticket médio e distorce o
   acompanhamento de meta. Isso é aplicado de forma consistente em TODAS as
   tabelas Diamond que partem de fact_sales (nenhuma mistura de critério).
2) As 3 linhas órfãs da nota InvoiceID=500 (quarentenada na Silver por data
   futura — ver nota de topo de 03_model_gold.py): têm invoice_status NULO
   (não 'Emitida'), então já são excluídas automaticamente pelo mesmo filtro
   `invoice_status = 'Emitida'` acima. Não precisam de tratamento separado.

Filtro único, aplicado no início do pipeline (função `sales_base`):
`invoice_status = 'Emitida'` — cobre os dois casos ao mesmo tempo. Resultado:
1236 linhas entram nas agregações Diamond (1302 - 63 canceladas - 3 órfãs).

gold.fact_returns NÃO tem invoice_status (é uma tabela independente, no grão
de devolução) — todas as 95 linhas são usadas nos indicadores de devolução
sem filtro adicional. Isso é uma limitação assumida: uma devolução vinculada
a uma nota cancelada (se existir na origem) ainda entraria no numerador de
`percentual_devolucao`, enquanto a nota cancelada não entra no denominador
(receita_bruta). Não observado nos dados atuais; documentado para o caso de
a origem vir a registrar esse cenário no futuro.

--------------------------------------------------------------------------
DECISÃO DE MODELAGEM — granularidade de cada tabela Diamond
--------------------------------------------------------------------------
- diamond.monthly_sales: grão (year_month, region_key). Região do VENDEDOR
  (mesma semântica de fact_sales — ver decisão em 03_model_gold.py), não
  grão diário nem por vendedor individual: fino o suficiente para dar um
  gráfico de tendência mensal por região sem precisar de DAX, grosso o
  suficiente para não virar uma cópia agregada de fact_sales. Colunas de
  calendário (year, month, month_name) são denormalizadas da gold.dim_date
  para o Power BI plotar a série temporal direto, sem precisar relacionar
  com dim_date (que é diária — relacionar uma tabela mensal a uma dimensão
  diária pelo year_month geraria relacionamento 1:N mal definido no modelo
  estrela do Power BI).
- diamond.product_ranking / diamond.customer_ranking /
  diamond.salesperson_performance: grão = 1 linha por entidade (produto/
  cliente/vendedor), consolidando TODO o período disponível. Atributos
  descritivos (nome, categoria, segmento etc.) são denormalizados da
  respectiva dim_* da Gold para a tabela funcionar sozinha em uma visual de
  tabela/gráfico de barras do Power BI, sem exigir join. As colunas de
  chave (*_key) são preservadas para quem quiser relacionar com a dim_*
  correspondente (relação 1:1, sem risco de muitos-para-muitos).
- diamond.target_vs_actual: grão (salesperson_key, region_key, target_year,
  target_month) — mesmo grão de gold.fact_sales_targets (a tabela mais fina
  já disponível para meta). FULL OUTER JOIN entre meta e realizado: preserva
  metas sem venda no mês (realizado = 0) e vendas sem meta cadastrada
  (percentual_atingimento_meta = NULL, não 0 nem erro).
- diamond.commercial_kpis: grão = 1 linha por mês (year_month), visão
  executiva de topo consolidada em nível de empresa (sem quebra por região/
  vendedor) — pensada para cards e para um gráfico de tendência único no
  topo do dashboard. Quebras por região/vendedor/produto já existem nas
  outras tabelas Diamond; replicá-las aqui geraria redundância sem ganho
  para o caso de uso "KPI executivo de topo".
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.config import get_table_path
from src.utils import get_logger, get_spark_session

logger = get_logger("create_diamond")

# COMMAND ----------
# Helpers
# COMMAND ----------


def read_gold(spark, table):
    return spark.read.format("delta").load(get_table_path("gold", table))


def write_diamond(df, table_name):
    path = get_table_path("diamond", table_name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    count = df.count()
    logger.info(f"[diamond.{table_name}] gravado — {count} linhas")
    return count


def sales_base(fact_sales):
    """
    Base filtrada de gold.fact_sales usada por TODAS as agregações
    executivas da Diamond. Ver decisão de modelagem no topo do módulo:
    exclui invoice_status != 'Emitida' (cobre canceladas + os 3 itens órfãos
    da nota quarentenada InvoiceID=500, que ficam com invoice_status nulo).
    """
    return fact_sales.filter(F.col("invoice_status") == "Emitida")


# COMMAND ----------
# 1. Vendas mensais
# COMMAND ----------


def build_monthly_sales(spark, base, dim_date, dim_region):
    base_with_date = base.join(
        dim_date.select("date_key", "year", "month", "month_name", "year_month"), "date_key", "left"
    )

    agg = base_with_date.groupBy("year_month", "year", "month", "month_name", "region_key").agg(
        F.sum("receita_bruta").alias("receita_bruta"),
        F.sum("receita_liquida").alias("receita_liquida"),
        F.sum("margem_valor").alias("margem_valor"),
        F.sum("valor_desconto").alias("valor_desconto"),
        F.sum("valor_devolucao").alias("valor_devolucao"),
        F.sum("quantity").alias("quantidade_vendida"),
        F.countDistinct("invoice_id").alias("quantidade_notas"),
    )

    agg = agg.withColumn(
        "margem_percentual",
        F.when(F.col("receita_liquida") != 0, F.col("margem_valor") / F.col("receita_liquida")).otherwise(
            F.lit(None).cast("double")
        ),
    )
    agg = agg.withColumn(
        "ticket_medio",
        F.when(F.col("quantidade_notas") != 0, F.col("receita_liquida") / F.col("quantidade_notas")).otherwise(
            F.lit(None).cast("double")
        ),
    )

    agg = agg.join(dim_region.select("region_key", "region_name", "region_code"), "region_key", "left")

    monthly_sales = agg.select(
        "year_month",
        "year",
        "month",
        "month_name",
        "region_key",
        "region_code",
        "region_name",
        "receita_bruta",
        "receita_liquida",
        "valor_desconto",
        "valor_devolucao",
        "margem_valor",
        "margem_percentual",
        "quantidade_vendida",
        "quantidade_notas",
        "ticket_medio",
    ).orderBy("year_month", "region_name")

    write_diamond(monthly_sales, "monthly_sales")
    return monthly_sales


# COMMAND ----------
# 2. Ranking de produtos
# COMMAND ----------


def build_product_ranking(spark, base, fact_returns, dim_product):
    sales_agg = base.groupBy("product_key").agg(
        F.sum("receita_bruta").alias("receita_bruta"),
        F.sum("receita_liquida").alias("receita_liquida"),
        F.sum("margem_valor").alias("margem_valor"),
        F.sum("quantity").alias("quantidade_vendida"),
        F.countDistinct("invoice_id").alias("quantidade_notas"),
    )

    returns_agg = fact_returns.groupBy("product_key").agg(
        F.sum("valor_devolvido").alias("valor_devolvido"),
        F.sum("quantity").alias("quantidade_devolvida"),
    )

    ranking = sales_agg.join(returns_agg, "product_key", "left").fillna(
        {"valor_devolvido": 0.0, "quantidade_devolvida": 0.0}
    )

    ranking = ranking.withColumn(
        "margem_percentual",
        F.when(F.col("receita_liquida") != 0, F.col("margem_valor") / F.col("receita_liquida")).otherwise(
            F.lit(None).cast("double")
        ),
    )
    ranking = ranking.withColumn(
        "percentual_devolucao",
        F.when(F.col("receita_bruta") != 0, F.col("valor_devolvido") / F.col("receita_bruta")).otherwise(
            F.lit(None).cast("double")
        ),
    )

    ranking = ranking.join(
        dim_product.select("product_key", "product_id", "product_code", "product_name", "category_name"),
        "product_key",
        "left",
    )

    ranking = ranking.withColumn("rank_receita_liquida", F.dense_rank().over(Window.orderBy(F.desc("receita_liquida"))))
    ranking = ranking.withColumn("rank_margem_valor", F.dense_rank().over(Window.orderBy(F.desc("margem_valor"))))
    ranking = ranking.withColumn(
        "rank_quantidade_vendida", F.dense_rank().over(Window.orderBy(F.desc("quantidade_vendida")))
    )
    ranking = ranking.withColumn(
        "rank_valor_devolvido", F.dense_rank().over(Window.orderBy(F.desc("valor_devolvido")))
    )

    product_ranking = ranking.select(
        "product_key",
        "product_id",
        "product_code",
        "product_name",
        "category_name",
        "receita_bruta",
        "receita_liquida",
        "margem_valor",
        "margem_percentual",
        "quantidade_vendida",
        "quantidade_notas",
        "valor_devolvido",
        "quantidade_devolvida",
        "percentual_devolucao",
        "rank_receita_liquida",
        "rank_margem_valor",
        "rank_quantidade_vendida",
        "rank_valor_devolvido",
    ).orderBy("rank_receita_liquida")

    write_diamond(product_ranking, "product_ranking")
    return product_ranking


# COMMAND ----------
# 3. Ranking de clientes
# COMMAND ----------


def build_customer_ranking(spark, base, fact_returns, dim_customer):
    sales_agg = base.groupBy("customer_key").agg(
        F.sum("receita_bruta").alias("receita_bruta"),
        F.sum("receita_liquida").alias("receita_liquida"),
        F.sum("margem_valor").alias("margem_valor"),
        F.sum("quantity").alias("quantidade_vendida"),
        F.countDistinct("invoice_id").alias("quantidade_notas"),
    )

    returns_agg = fact_returns.groupBy("customer_key").agg(
        F.sum("valor_devolvido").alias("valor_devolvido"),
        F.sum("quantity").alias("quantidade_devolvida"),
    )

    ranking = sales_agg.join(returns_agg, "customer_key", "left").fillna(
        {"valor_devolvido": 0.0, "quantidade_devolvida": 0.0}
    )

    ranking = ranking.withColumn(
        "ticket_medio",
        F.when(F.col("quantidade_notas") != 0, F.col("receita_liquida") / F.col("quantidade_notas")).otherwise(
            F.lit(None).cast("double")
        ),
    )
    ranking = ranking.withColumn(
        "percentual_devolucao",
        F.when(F.col("receita_bruta") != 0, F.col("valor_devolvido") / F.col("receita_bruta")).otherwise(
            F.lit(None).cast("double")
        ),
    )

    ranking = ranking.join(
        dim_customer.select(
            "customer_key", "customer_id", "customer_code", "customer_name", "customer_segment", "region_name"
        ),
        "customer_key",
        "left",
    )

    ranking = ranking.withColumn("rank_receita_liquida", F.dense_rank().over(Window.orderBy(F.desc("receita_liquida"))))
    ranking = ranking.withColumn(
        "rank_valor_devolvido", F.dense_rank().over(Window.orderBy(F.desc("valor_devolvido")))
    )

    customer_ranking = ranking.select(
        "customer_key",
        "customer_id",
        "customer_code",
        "customer_name",
        "customer_segment",
        "region_name",
        "receita_bruta",
        "receita_liquida",
        "margem_valor",
        "quantidade_vendida",
        "quantidade_notas",
        "ticket_medio",
        "valor_devolvido",
        "quantidade_devolvida",
        "percentual_devolucao",
        "rank_receita_liquida",
        "rank_valor_devolvido",
    ).orderBy("rank_receita_liquida")

    write_diamond(customer_ranking, "customer_ranking")
    return customer_ranking


# COMMAND ----------
# 4. Performance de vendedores
# COMMAND ----------


def build_salesperson_performance(spark, base, fact_returns, dim_salesperson):
    sales_agg = base.groupBy("salesperson_key").agg(
        F.sum("receita_bruta").alias("receita_bruta"),
        F.sum("receita_liquida").alias("receita_liquida"),
        F.sum("margem_valor").alias("margem_valor"),
        F.sum("quantity").alias("quantidade_vendida"),
        F.countDistinct("invoice_id").alias("quantidade_notas"),
    )

    returns_agg = fact_returns.groupBy("salesperson_key").agg(
        F.sum("valor_devolvido").alias("valor_devolvido"),
        F.sum("quantity").alias("quantidade_devolvida"),
    )

    perf = sales_agg.join(returns_agg, "salesperson_key", "left").fillna(
        {"valor_devolvido": 0.0, "quantidade_devolvida": 0.0}
    )

    perf = perf.withColumn(
        "margem_percentual",
        F.when(F.col("receita_liquida") != 0, F.col("margem_valor") / F.col("receita_liquida")).otherwise(
            F.lit(None).cast("double")
        ),
    )
    perf = perf.withColumn(
        "ticket_medio",
        F.when(F.col("quantidade_notas") != 0, F.col("receita_liquida") / F.col("quantidade_notas")).otherwise(
            F.lit(None).cast("double")
        ),
    )
    perf = perf.withColumn(
        "percentual_devolucao",
        F.when(F.col("receita_bruta") != 0, F.col("valor_devolvido") / F.col("receita_bruta")).otherwise(
            F.lit(None).cast("double")
        ),
    )

    perf = perf.join(
        dim_salesperson.select(
            "salesperson_key", "salesperson_id", "salesperson_code", "salesperson_name", "region_name"
        ),
        "salesperson_key",
        "left",
    )

    perf = perf.withColumn("rank_receita_liquida", F.dense_rank().over(Window.orderBy(F.desc("receita_liquida"))))
    perf = perf.withColumn("rank_valor_devolvido", F.dense_rank().over(Window.orderBy(F.desc("valor_devolvido"))))

    salesperson_performance = perf.select(
        "salesperson_key",
        "salesperson_id",
        "salesperson_code",
        "salesperson_name",
        "region_name",
        "receita_bruta",
        "receita_liquida",
        "margem_valor",
        "margem_percentual",
        "quantidade_vendida",
        "quantidade_notas",
        "ticket_medio",
        "valor_devolvido",
        "quantidade_devolvida",
        "percentual_devolucao",
        "rank_receita_liquida",
        "rank_valor_devolvido",
    ).orderBy("rank_receita_liquida")

    write_diamond(salesperson_performance, "salesperson_performance")
    return salesperson_performance


# COMMAND ----------
# 5. Meta x realizado
# COMMAND ----------


def build_target_vs_actual(spark, base, dim_date, fact_sales_targets, dim_salesperson, dim_region):
    base_with_date = base.join(dim_date.select("date_key", "year", "month"), "date_key", "left")

    realized = base_with_date.groupBy(
        "salesperson_key", "region_key", F.col("year").alias("target_year"), F.col("month").alias("target_month")
    ).agg(
        F.sum("receita_liquida").alias("receita_liquida_realizada"),
        F.sum("margem_valor").alias("margem_valor_realizada"),
    )

    targets = fact_sales_targets.select(
        "salesperson_key", "region_key", "target_year", "target_month", "target_value"
    )

    joined = targets.join(
        realized, ["salesperson_key", "region_key", "target_year", "target_month"], "full_outer"
    )

    joined = joined.fillna({"receita_liquida_realizada": 0.0, "margem_valor_realizada": 0.0})

    joined = joined.withColumn(
        "percentual_atingimento_meta",
        F.when(
            F.col("target_value").isNotNull() & (F.col("target_value") != 0),
            F.col("receita_liquida_realizada") / F.col("target_value"),
        ).otherwise(F.lit(None).cast("double")),
    )
    joined = joined.withColumn("tem_meta_cadastrada", F.col("target_value").isNotNull())

    joined = joined.join(
        dim_salesperson.select("salesperson_key", "salesperson_code", "salesperson_name"), "salesperson_key", "left"
    )
    joined = joined.join(dim_region.select("region_key", "region_code", "region_name"), "region_key", "left")

    target_vs_actual = joined.select(
        "salesperson_key",
        "salesperson_code",
        "salesperson_name",
        "region_key",
        "region_code",
        "region_name",
        "target_year",
        "target_month",
        "target_value",
        "receita_liquida_realizada",
        "margem_valor_realizada",
        "percentual_atingimento_meta",
        "tem_meta_cadastrada",
    ).orderBy("target_year", "target_month", "salesperson_name")

    write_diamond(target_vs_actual, "target_vs_actual")
    return target_vs_actual


# COMMAND ----------
# 6. KPIs comerciais consolidados (visão executiva de topo)
# COMMAND ----------


def build_commercial_kpis(spark, base, dim_date, fact_returns, target_vs_actual):
    base_with_date = base.join(
        dim_date.select("date_key", "year", "month", "month_name", "year_month"), "date_key", "left"
    )

    sales_agg = base_with_date.groupBy("year_month", "year", "month", "month_name").agg(
        F.sum("receita_bruta").alias("receita_bruta"),
        F.sum("receita_liquida").alias("receita_liquida"),
        F.sum("margem_valor").alias("margem_valor"),
        F.sum("quantity").alias("quantidade_vendida"),
        F.countDistinct("invoice_id").alias("quantidade_notas"),
        F.countDistinct("customer_key").alias("quantidade_clientes_ativos"),
        F.countDistinct("salesperson_key").alias("quantidade_vendedores_ativos"),
    )

    # Devoluções por mês: fact_returns não tem invoice_status (ver decisão de
    # modelagem no topo do módulo) — usa date_key da própria devolução.
    returns_agg = fact_returns.join(dim_date.select("date_key", "year_month"), "date_key", "left").groupBy(
        "year_month"
    ).agg(F.sum("valor_devolvido").alias("valor_devolvido"))

    # Meta total do mês: soma de target_value por year_month, a partir do
    # target_vs_actual já calculado (reaproveita, não reagrega de novo).
    target_agg = target_vs_actual.groupBy(
        F.format_string("%04d-%02d", F.col("target_year"), F.col("target_month")).alias("year_month")
    ).agg(
        F.sum("target_value").alias("valor_meta_total"),
        F.sum("receita_liquida_realizada").alias("_receita_liquida_via_meta"),
    )

    kpis = sales_agg.join(returns_agg, "year_month", "left").join(target_agg, "year_month", "left")
    kpis = kpis.fillna({"valor_devolvido": 0.0, "valor_meta_total": 0.0})

    kpis = kpis.withColumn(
        "margem_percentual",
        F.when(F.col("receita_liquida") != 0, F.col("margem_valor") / F.col("receita_liquida")).otherwise(
            F.lit(None).cast("double")
        ),
    )
    kpis = kpis.withColumn(
        "ticket_medio",
        F.when(F.col("quantidade_notas") != 0, F.col("receita_liquida") / F.col("quantidade_notas")).otherwise(
            F.lit(None).cast("double")
        ),
    )
    kpis = kpis.withColumn(
        "percentual_devolucao",
        F.when(F.col("receita_bruta") != 0, F.col("valor_devolvido") / F.col("receita_bruta")).otherwise(
            F.lit(None).cast("double")
        ),
    )
    kpis = kpis.withColumn(
        "percentual_atingimento_meta",
        F.when(
            F.col("valor_meta_total").isNotNull() & (F.col("valor_meta_total") != 0),
            F.col("receita_liquida") / F.col("valor_meta_total"),
        ).otherwise(F.lit(None).cast("double")),
    )

    commercial_kpis = kpis.select(
        "year_month",
        "year",
        "month",
        "month_name",
        "receita_bruta",
        "receita_liquida",
        "margem_valor",
        "margem_percentual",
        "quantidade_vendida",
        "quantidade_notas",
        "ticket_medio",
        "quantidade_clientes_ativos",
        "quantidade_vendedores_ativos",
        "valor_devolvido",
        "percentual_devolucao",
        "valor_meta_total",
        "percentual_atingimento_meta",
    ).orderBy("year_month")

    write_diamond(commercial_kpis, "commercial_kpis")
    return commercial_kpis


# COMMAND ----------
# Validação (sanity checks contra a Gold)
# COMMAND ----------


def validate_diamond(spark, base, monthly_sales, product_ranking, customer_ranking, salesperson_performance,
                      target_vs_actual, commercial_kpis):
    logger.info("== Validação Diamond ==")
    ok = True
    tol = 0.01  # tolerância de arredondamento (ponto flutuante)

    total_receita_base = base.agg(F.sum("receita_liquida")).collect()[0][0]

    checks = {
        "monthly_sales": monthly_sales.agg(F.sum("receita_liquida")).collect()[0][0],
        "product_ranking": product_ranking.agg(F.sum("receita_liquida")).collect()[0][0],
        "customer_ranking": customer_ranking.agg(F.sum("receita_liquida")).collect()[0][0],
        "salesperson_performance": salesperson_performance.agg(F.sum("receita_liquida")).collect()[0][0],
    }
    for table_name, total in checks.items():
        diff = abs((total or 0) - total_receita_base)
        status = "OK" if diff <= tol else "MISMATCH"
        if diff > tol:
            ok = False
        logger.info(
            f"[conferência receita_liquida] fact_sales(Emitida)={total_receita_base:.2f} "
            f"diamond.{table_name}={total:.2f} diff={diff:.4f} -> {status}"
        )

    # quantidade_notas de monthly_sales somada não deve ser comparada
    # diretamente com distinct global (uma mesma nota pode aparecer em mais
    # de um mês/região? não — 1 nota tem 1 issue_date e 1 vendedor, logo 1
    # region_key e 1 year_month), então a soma por grão deve bater com o
    # distinct global de invoice_id.
    distinct_invoices_base = base.select("invoice_id").distinct().count()
    sum_quantidade_notas = monthly_sales.agg(F.sum("quantidade_notas")).collect()[0][0]
    status = "OK" if sum_quantidade_notas == distinct_invoices_base else "MISMATCH"
    if sum_quantidade_notas != distinct_invoices_base:
        ok = False
    logger.info(
        f"[conferência quantidade_notas] distinct(invoice_id) fact_sales(Emitida)={distinct_invoices_base} "
        f"sum(monthly_sales.quantidade_notas)={sum_quantidade_notas} -> {status}"
    )

    # commercial_kpis: receita_liquida mensal deve bater com monthly_sales
    # agregado por year_month (sem quebra de região).
    monthly_total = monthly_sales.groupBy("year_month").agg(F.sum("receita_liquida").alias("receita_liquida"))
    kpi_vs_monthly = commercial_kpis.select("year_month", "receita_liquida").join(
        monthly_total.withColumnRenamed("receita_liquida", "receita_liquida_monthly"), "year_month", "inner"
    )
    mismatches = kpi_vs_monthly.filter(
        F.abs(F.col("receita_liquida") - F.col("receita_liquida_monthly")) > tol
    ).count()
    status = "OK" if mismatches == 0 else "MISMATCH"
    if mismatches > 0:
        ok = False
    logger.info(f"[conferência commercial_kpis x monthly_sales] meses divergentes={mismatches} -> {status}")

    # target_vs_actual: nenhuma linha com meta cadastrada deve ficar sem
    # percentual_atingimento_meta calculado.
    broken_pct = target_vs_actual.filter(
        F.col("tem_meta_cadastrada") & F.col("percentual_atingimento_meta").isNull()
    ).count()
    status = "OK" if broken_pct == 0 else "MISMATCH"
    if broken_pct > 0:
        ok = False
    logger.info(f"[conferência target_vs_actual] metas cadastradas sem percentual calculado={broken_pct} -> {status}")

    logger.info(f"== Validação Diamond: {'PASSOU' if ok else 'FALHOU'} ==")
    return ok


# COMMAND ----------


def main():
    spark = get_spark_session("erp-create-diamond")
    try:
        fact_sales = read_gold(spark, "fact_sales")
        fact_returns = read_gold(spark, "fact_returns")
        fact_sales_targets = read_gold(spark, "fact_sales_targets")
        dim_date = read_gold(spark, "dim_date")
        dim_region = read_gold(spark, "dim_region")
        dim_product = read_gold(spark, "dim_product")
        dim_customer = read_gold(spark, "dim_customer")
        dim_salesperson = read_gold(spark, "dim_salesperson")

        base = sales_base(fact_sales).cache()
        logger.info(f"[sales_base] {base.count()} linhas (invoice_status='Emitida', órfãos excluídos)")

        monthly_sales = build_monthly_sales(spark, base, dim_date, dim_region)
        product_ranking = build_product_ranking(spark, base, fact_returns, dim_product)
        customer_ranking = build_customer_ranking(spark, base, fact_returns, dim_customer)
        salesperson_performance = build_salesperson_performance(spark, base, fact_returns, dim_salesperson)
        target_vs_actual = build_target_vs_actual(
            spark, base, dim_date, fact_sales_targets, dim_salesperson, dim_region
        )
        commercial_kpis = build_commercial_kpis(spark, base, dim_date, fact_returns, target_vs_actual)

        validate_diamond(
            spark, base, monthly_sales, product_ranking, customer_ranking, salesperson_performance,
            target_vs_actual, commercial_kpis
        )

        base.unpersist()
    finally:
        spark.stop()

    logger.info("Camada Diamond finalizada")


if __name__ == "__main__":
    main()
