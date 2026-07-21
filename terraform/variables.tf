variable "aws_region" {
  description = "AWS region in which the aviation platform resources are deployed."
  type        = string
  default     = "eu-central-1"
}

variable "project_name" {
  description = "Name used to identify aviation platform resources."
  type        = string
  default     = "aviation-operations-platform"
}

variable "environment" {
  description = "Deployment environment."
  type        = string
  default     = "dev"

  validation {
    condition = contains(
      ["dev", "test", "prod"],
      var.environment
    )

    error_message = "Environment must be dev, test, or prod."
  }
}

variable "raw_bucket_name" {
  description = "Globally unique S3 bucket name for raw and processed aviation data."
  type        = string
}

variable "glue_database_name" {
  description = "AWS Glue database used for aviation Iceberg tables."
  type        = string
  default     = "aviation_operations"
}