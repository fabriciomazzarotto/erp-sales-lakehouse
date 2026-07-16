variable "aws_region" {
  description = "Região AWS onde a infraestrutura do Lakehouse é provisionada."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Ambiente (dev/prod) — usado para nomear/isolar recursos."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment deve ser \"dev\" ou \"prod\"."
  }
}

variable "project_name" {
  description = "Prefixo usado no nome de todos os recursos do projeto."
  type        = string
  default     = "erp-sales-lakehouse"
}

variable "glue_database_name" {
  description = "Nome do database no Glue Data Catalog (mesmo valor de GLUE_DATABASE_NAME no .env)."
  type        = string
  default     = "erp_sales_lakehouse"
}

variable "lakehouse_layers" {
  description = "Camadas do Lakehouse — cada uma vira um bucket S3 + database/crawler no Glue."
  type        = list(string)
  default     = ["bronze", "silver", "gold", "diamond"]
}

variable "databricks_pipeline_role_trust_arn" {
  description = <<-EOT
    ARN da entidade autorizada a assumir a role de execução do pipeline
    (ex.: instance profile do Databricks, ou a conta/role do usuário local
    enquanto não há um workspace Databricks real). Deixado como variável
    (sem default) de propósito — deve ser definido no terraform.tfvars no
    momento do deploy real, nunca hardcoded aqui.
  EOT
  type        = string
  default     = null
}
