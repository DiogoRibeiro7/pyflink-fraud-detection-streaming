variable "aws_region" {
  description = "AWS region for the deployment."
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Short project slug used in names and tags."
  type        = string
  default     = "pyflink-fraud-detection-streaming"
}

variable "environment" {
  description = "Environment name such as dev, staging, or prod."
  type        = string
  default     = "dev"
}

variable "vpc_id" {
  description = "Target VPC identifier."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs used by streaming workloads."
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security groups attached to Flink or client workloads."
  type        = list(string)
  default     = []
}

variable "raw_transactions_bucket" {
  description = "S3 bucket used for raw transactions or staging."
  type        = string
}

variable "alerts_bucket" {
  description = "S3 bucket used for fraud alerts or downstream analytical export."
  type        = string
}

variable "glue_database_name" {
  description = "Glue database name for analytical tables."
  type        = string
  default     = "fraud_streaming"
}

variable "transactions_topic_name" {
  description = "Kafka topic or logical stream name for incoming transactions."
  type        = string
  default     = "transactions"
}

variable "alerts_topic_name" {
  description = "Kafka topic or logical stream name for emitted alerts."
  type        = string
  default     = "fraud-alerts"
}

variable "extra_tags" {
  description = "Additional resource tags."
  type        = map(string)
  default     = {}
}
