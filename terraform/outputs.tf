output "aviation_bucket_name" {
  description = "Name of the existing aviation data platform S3 bucket."
  value       = data.aws_s3_bucket.aviation_bucket.id
}

output "aviation_bucket_arn" {
  description = "ARN of the existing aviation data platform S3 bucket."
  value       = data.aws_s3_bucket.aviation_bucket.arn
}

output "glue_database_name" {
  description = "Name of the existing AWS Glue database."
  value       = aws_glue_catalog_database.aviation_database.name
}

output "glue_database_arn" {
  description = "ARN of the existing AWS Glue database."
  value       = aws_glue_catalog_database.aviation_database.arn
}

output "emr_ec2_role_name" {
  description = "Name of the IAM role used by EMR EC2 instances."
  value       = aws_iam_role.emr_ec2_role.name
}

output "emr_ec2_role_arn" {
  description = "ARN of the IAM role used by EMR EC2 instances."
  value       = aws_iam_role.emr_ec2_role.arn
}

output "emr_ec2_instance_profile_name" {
  description = "Instance profile supplied when launching an EMR cluster."
  value       = aws_iam_instance_profile.emr_ec2_profile.name
}

output "emr_data_access_policy_arn" {
  description = "ARN of the aviation EMR data-access policy."
  value       = aws_iam_policy.emr_data_access.arn
}