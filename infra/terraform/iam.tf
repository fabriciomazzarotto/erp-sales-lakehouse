data "aws_caller_identity" "current" {}

locals {
  # Placeholder de confiança: assume a própria conta enquanto não há um
  # workspace Databricks real (cross-account role ARN) configurado via
  # var.databricks_pipeline_role_trust_arn. NUNCA usar "*" aqui — mesmo como
  # placeholder, a role já nasceria assumível por qualquer principal da AWS.
  pipeline_trust_principal = coalesce(
    var.databricks_pipeline_role_trust_arn,
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
  )

  lakehouse_bucket_arns = [for b in aws_s3_bucket.lakehouse : b.arn]
}

# ---------------------------------------------------------------------------
# Role de execução do pipeline (Databricks) — least privilege: só as 4
# buckets do Lakehouse, nunca s3:*/"Resource": "*". Um único role cobre
# Bronze/Silver/Gold/Diamond porque, na prática, um cluster/job Databricks
# roda os 4 notebooks em sequência sob a mesma identidade; separar por
# camada exigiria um cluster/job por estágio, complexidade operacional não
# justificada na escala deste projeto — trade-off documentado, não omitido.
# ---------------------------------------------------------------------------
resource "aws_iam_role" "pipeline_execution" {
  name = "${var.project_name}-pipeline-execution-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = local.pipeline_trust_principal }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "pipeline_lakehouse_rw" {
  name        = "${var.project_name}-pipeline-lakehouse-rw-${var.environment}"
  description = "Leitura/escrita restrita aos 4 buckets do Lakehouse (Bronze/Silver/Gold/Diamond) — nada além disso."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListLakehouseBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.lakehouse_bucket_arns
      },
      {
        Sid    = "ReadWriteLakehouseObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = [for arn in local.lakehouse_bucket_arns : "${arn}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "pipeline_lakehouse_rw" {
  role       = aws_iam_role.pipeline_execution.name
  policy_arn = aws_iam_policy.pipeline_lakehouse_rw.arn
}

# ---------------------------------------------------------------------------
# Role do Glue Crawler — só leitura no S3 (nunca precisa escrever dado) +
# permissão de catalogar (Glue Data Catalog). Trust principal é o próprio
# serviço Glue (glue.amazonaws.com), não uma conta/role externa.
# ---------------------------------------------------------------------------
resource "aws_iam_role" "glue_crawler" {
  name = "${var.project_name}-glue-crawler-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "glue.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "glue_crawler_read_lakehouse" {
  name        = "${var.project_name}-glue-crawler-read-${var.environment}"
  description = "Leitura apenas (S3) para o Glue Crawler catalogar as tabelas do Lakehouse."

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListLakehouseBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = local.lakehouse_bucket_arns
      },
      {
        Sid      = "ReadLakehouseObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = [for arn in local.lakehouse_bucket_arns : "${arn}/*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_crawler_read_lakehouse" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = aws_iam_policy.glue_crawler_read_lakehouse.arn
}

# Permissão de catalogação (criar/atualizar tabelas e partições no database
# do projeto) — política gerenciada pela própria AWS, escopo já é razoável
# para um crawler (não temos motivo para reescrevê-la à mão).
resource "aws_iam_role_policy_attachment" "glue_crawler_service_role" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
