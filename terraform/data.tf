data "aws_caller_identity" "current" {}

data "aws_s3_bucket" "aviation_bucket" {
  bucket = var.raw_bucket_name
}

resource "aws_glue_catalog_database" "aviation_database" {
  name = var.glue_database_name

  lifecycle {
    prevent_destroy = true
  }
}