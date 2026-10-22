# AWS equivalents of the local lakehouse: S3 for Iceberg data, Glue as the
# Iceberg catalog, and EMR Serverless to run the Spark jobs. Kafka (MSK) and
# Airflow (MWAA) are left out to keep the bill near zero for a demo.

data "aws_caller_identity" "current" {}

locals {
  lake_bucket = "${var.project}-lake-${data.aws_caller_identity.current.account_id}"
}

# ---------- storage ----------
resource "aws_s3_bucket" "lake" {
  bucket = local.lake_bucket
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_days
    }
  }

  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }
}

# ---------- catalog ----------
resource "aws_glue_catalog_database" "layers" {
  for_each     = toset(["bronze", "silver", "gold", "marts"])
  name         = "${var.project}_${each.key}"
  location_uri = "s3://${aws_s3_bucket.lake.bucket}/warehouse/${each.key}"
}

# ---------- compute ----------
resource "aws_emrserverless_application" "spark" {
  name          = "${var.project}-spark"
  release_label = var.emr_release
  type          = "spark"

  maximum_capacity {
    cpu    = "16 vCPU"
    memory = "64 GB"
  }

  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }
}

data "aws_iam_policy_document" "emr_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "spark_job" {
  name               = "${var.project}-spark-job"
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

data "aws_iam_policy_document" "spark_job" {
  statement {
    sid       = "LakeReadWrite"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*"]
  }

  statement {
    sid = "GlueCatalog"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartitions",
    ]

    resources = concat(
      ["arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog"],
      [for db in aws_glue_catalog_database.layers : db.arn],
      [for db in aws_glue_catalog_database.layers : "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${db.name}/*"],
    )
  }
}

resource "aws_iam_role_policy" "spark_job" {
  name   = "lake-access"
  role   = aws_iam_role.spark_job.id
  policy = data.aws_iam_policy_document.spark_job.json
}
