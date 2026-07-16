# Workgroup dedicado ao projeto — evita usar o workgroup "primary" da conta
# (compartilhado, sem isolamento de custo/config entre projetos diferentes).
resource "aws_athena_workgroup" "lakehouse" {
  name = "${var.project_name}-${var.environment}"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    # Corta a query automaticamente se ela for escanear mais que 5 GB —
    # trava de segurança de custo contra um SELECT * sem WHERE numa tabela
    # que cresça muito, sem depender de disciplina manual de quem consulta.
    bytes_scanned_cutoff_per_query = 5368709120 # 5 GB

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
