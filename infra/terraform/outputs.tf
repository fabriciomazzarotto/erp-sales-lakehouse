output "lakehouse_bucket_names" {
  description = "Nome dos buckets S3 por camada — usar para preencher AWS_S3_BUCKET_* no .env."
  value       = { for layer, bucket in aws_s3_bucket.lakehouse : layer => bucket.bucket }
}

output "athena_results_bucket" {
  description = "Bucket de resultados de query do Athena — usar para preencher ATHENA_OUTPUT_S3 no .env."
  value       = "s3://${aws_s3_bucket.athena_results.bucket}/"
}

output "glue_database_name" {
  description = "Database no Glue Data Catalog — usar para preencher GLUE_DATABASE_NAME no .env."
  value       = aws_glue_catalog_database.lakehouse.name
}

output "athena_workgroup_name" {
  description = "Workgroup do Athena a ser usado nas queries (não o \"primary\")."
  value       = aws_athena_workgroup.lakehouse.name
}

output "pipeline_execution_role_arn" {
  description = "ARN da role a anexar ao cluster/job Databricks (instance profile) para ler/gravar o Lakehouse."
  value       = aws_iam_role.pipeline_execution.arn
}

output "glue_crawler_role_arn" {
  description = "ARN da role usada pelos Glue Crawlers."
  value       = aws_iam_role.glue_crawler.arn
}
