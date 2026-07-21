# Trust policy
# Allows EC2 instances in the EMR cluster to assume the role.

data "aws_iam_policy_document" "emr_ec2_assume_role" {
  statement {
    effect = "Allow"

    actions = [
      "sts:AssumeRole"
    ]

    principals {
      type = "Service"

      identifiers = [
        "ec2.amazonaws.com"
      ]
    }
  }
}

# EMR EC2 runtime role

resource "aws_iam_role" "emr_ec2_role" {
  name = "${var.project_name}-emr-ec2-role"

  description = "Runtime role used by EMR Spark jobs for the aviation data platform."

  assume_role_policy = data.aws_iam_policy_document.emr_ec2_assume_role.json

  lifecycle {
    prevent_destroy = true
  }
}


# Least-privilege data access policy

data "aws_iam_policy_document" "emr_data_access" {

  # Read the bucket structure.
  statement {
    sid    = "ListAviationBucket"
    effect = "Allow"

    actions = [
      "s3:GetBucketLocation",
      "s3:ListBucket"
    ]

    resources = [
      data.aws_s3_bucket.aviation_bucket.arn
    ]
  }

  # Read and write aviation data, Spark outputs,
  # Iceberg metadata, checkpoints and EMR logs.

  statement {
    sid    = "ReadWriteAviationObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts"
    ]

    resources = [
      "${data.aws_s3_bucket.aviation_bucket.arn}/*"
    ]
  }

  # Read the Glue Data Catalog.

  statement {
    sid    = "ReadGlueCatalog"
    effect = "Allow"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:GetTags"
    ]

    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.aviation_database.arn,
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*"
    ]
  }

  # Allow Spark/Iceberg to create and update catalog tables.

  statement {
    sid    = "ManageAviationGlueTables"
    effect = "Allow"

    actions = [
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:CreatePartition",
      "glue:BatchCreatePartition",
      "glue:UpdatePartition",
      "glue:DeletePartition",
      "glue:BatchDeletePartition"
    ]

    resources = [
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:catalog",
      aws_glue_catalog_database.aviation_database.arn,
      "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:table/${var.glue_database_name}/*"
    ]
  }
}

# Customer-managed IAM policy

resource "aws_iam_policy" "emr_data_access" {
  name = "${var.project_name}-emr-data-access"

  description = "Allows aviation EMR Spark jobs to access S3 and AWS Glue."

  policy = data.aws_iam_policy_document.emr_data_access.json
}

# Attach the policy to the role

resource "aws_iam_role_policy_attachment" "emr_data_access" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = aws_iam_policy.emr_data_access.arn
}

# EMR requires an instance profile around the EC2 role.


resource "aws_iam_instance_profile" "emr_ec2_profile" {
  name = "${var.project_name}-emr-ec2-profile"
  role = aws_iam_role.emr_ec2_role.name
}