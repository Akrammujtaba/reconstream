output "lake_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "glue_databases" {
  value = [for db in aws_glue_catalog_database.layers : db.name]
}

output "emr_application_id" {
  value = aws_emrserverless_application.spark.id
}

output "spark_job_role_arn" {
  value = aws_iam_role.spark_job.arn
}
