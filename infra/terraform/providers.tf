terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # Estado local por padrão (projeto de portfólio, uso individual). Antes de
  # qualquer uso em equipe, migrar para backend remoto (S3 + DynamoDB lock):
  #
  # backend "s3" {
  #   bucket         = "erp-sales-lakehouse-terraform-state"
  #   key            = "erp-sales-lakehouse/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "erp-sales-lakehouse-terraform-locks"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "erp-sales-lakehouse"
      ManagedBy   = "terraform"
      Environment = var.environment
    }
  }
}
