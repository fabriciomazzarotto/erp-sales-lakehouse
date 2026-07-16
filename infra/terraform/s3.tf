# Nomes de bucket S3 são únicos GLOBALMENTE (todas as contas AWS do mundo) —
# "erp-sales-lakehouse-bronze" sozinho quase certamente já existe. Um sufixo
# aleatório resolve isso sem exigir coordenação manual a cada deploy.
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# Um bucket por camada do Lakehouse (bronze/silver/gold/diamond).
resource "aws_s3_bucket" "lakehouse" {
  for_each = toset(var.lakehouse_layers)

  bucket = "${var.project_name}-${each.key}-${var.environment}-${random_id.bucket_suffix.hex}"

  tags = {
    Layer = each.key
  }
}

# Bloqueia acesso público em todos os buckets — nenhuma camada do Lakehouse
# deve ser exposta diretamente à internet; consumo é sempre via
# IAM/Athena/Glue/Databricks com credenciais.
resource "aws_s3_bucket_public_access_block" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse

  bucket = each.value.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Criptografia em repouso por padrão (SSE-S3). Em produção com dado sensível
# de verdade, trocar por SSE-KMS com uma CMK dedicada (permite auditoria de
# uso da chave via CloudTrail, e revogação de acesso independente do IAM).
resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

# Versionamento como rede de segurança contra exclusão/sobrescrita acidental
# (o Delta Lake já tem seu próprio versionamento via transaction log, mas
# isso não protege contra alguém apagar o bucket/prefixo por engano).
resource "aws_s3_bucket_versioning" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse

  bucket = each.value.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Otimização de custo: dados históricos da Bronze são lidos com pouca
# frequência depois de processados para a Silver — mover para uma classe de
# armazenamento mais barata após 90 dias. Camadas Gold/Diamond são
# consultadas ativamente pelo Power BI/Athena, então não recebem essa regra.
resource "aws_s3_bucket_lifecycle_configuration" "bronze_cost_optimization" {
  bucket = aws_s3_bucket.lakehouse["bronze"].id

  rule {
    id     = "bronze-transition-to-ia"
    status = "Enabled"

    filter {}

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = 180
    }
  }
}

# Bucket separado para resultados de query do Athena (ATHENA_OUTPUT_S3 no
# .env) — nunca reaproveitar um bucket de dados para isso, mistura
# responsabilidades e dificulta política de retenção/custo.
resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_name}-athena-results-${var.environment}-${random_id.bucket_suffix.hex}"
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Resultados de query são transientes — expira em 7 dias para não acumular
# custo de armazenamento com dados que ninguém vai reconsultar.
resource "aws_s3_bucket_lifecycle_configuration" "athena_results_expiration" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {}

    expiration {
      days = 7
    }
  }
}
