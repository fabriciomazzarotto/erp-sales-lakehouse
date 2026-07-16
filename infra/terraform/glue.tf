# Um único database no Glue Data Catalog para o projeto inteiro — Athena
# consulta por database.table, então cada camada vira um prefixo de tabela
# (bronze_erp_customers, silver_customers, gold_dim_customer, ...) dentro do
# mesmo database, em vez de 4 databases separados. Mantém a navegação simples
# no Athena/Power BI sem perder a distinção de camada (visível no nome).
resource "aws_glue_catalog_database" "lakehouse" {
  name        = var.glue_database_name
  description = "Catálogo de metadados do ERP Sales Lakehouse (Bronze/Silver/Gold/Diamond)."
}

# Tabelas Delta Lake produzidas por cada camada (nomes de pasta = nomes de
# tabela, espelhando src/config.get_table_path). Mantido explícito em vez de
# "descobrir" via listagem de bucket porque um Glue crawler de Delta Lake
# precisa apontar para o PREFIXO DE CADA TABELA individualmente (delta_target
# não varre um bucket inteiro procurando tabelas como o s3_target genérico
# faz para Parquet solto) — atualizar esta lista ao adicionar uma tabela nova
# ao pipeline (notebooks/0X_*.py).
locals {
  lakehouse_tables_by_layer = {
    bronze = [
      "erp_regions", "erp_payment_methods", "erp_customers", "erp_products",
      "erp_salespersons", "erp_sales_invoice_header", "erp_sales_invoice_items",
      "erp_sales_returns", "erp_sales_targets",
    ]
    silver = [
      "regions", "payment_methods", "customers", "products", "salespersons",
      "sales_invoice_header", "sales_invoice_items", "sales_returns", "sales_targets",
    ]
    gold = [
      "dim_region", "dim_payment_method", "dim_customer", "dim_product",
      "dim_salesperson", "dim_date", "fact_sales", "fact_returns", "fact_sales_targets",
    ]
    diamond = [
      "monthly_sales", "product_ranking", "customer_ranking",
      "salesperson_performance", "target_vs_actual", "commercial_kpis",
    ]
  }
}

# Um crawler por camada, apontando para as tabelas Delta daquele bucket S3 —
# permite agendar/rodar a catalogação de cada camada de forma independente
# (ex.: recatalogar só a Diamond depois de uma mudança de schema).
resource "aws_glue_crawler" "lakehouse" {
  for_each = local.lakehouse_tables_by_layer

  name          = "${var.project_name}-${each.key}-crawler-${var.environment}"
  role          = aws_iam_role.glue_crawler.arn
  database_name = aws_glue_catalog_database.lakehouse.name

  delta_target {
    create_native_delta_table = true
    write_manifest            = false
    delta_tables = [
      for table_name in each.value :
      "s3://${aws_s3_bucket.lakehouse[each.key].bucket}/${table_name}/"
    ]
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}
