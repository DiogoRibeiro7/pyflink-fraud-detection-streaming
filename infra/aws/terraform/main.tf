terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"

  common_tags = merge(
    {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "terraform"
      Repository  = "pyflink-fraud-detection-streaming"
    },
    var.extra_tags
  )
}

# Template-only network data lookups.
data "aws_vpc" "selected" {
  id = var.vpc_id
}

data "aws_subnets" "selected" {
  filter {
    name   = "subnet-id"
    values = var.private_subnet_ids
  }
}

output "name_prefix" {
  value = local.name_prefix
}

output "private_subnet_ids" {
  value = data.aws_subnets.selected.ids
}

output "raw_transactions_bucket" {
  value = var.raw_transactions_bucket
}

output "alerts_bucket" {
  value = var.alerts_bucket
}
