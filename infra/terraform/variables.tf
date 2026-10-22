variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Name prefix for every resource."
  type        = string
  default     = "reconstream"
}

variable "emr_release" {
  description = "EMR Serverless release (Spark 3.5)."
  type        = string
  default     = "emr-7.2.0"
}

variable "noncurrent_version_days" {
  description = "Days to keep overwritten lake objects before deletion."
  type        = number
  default     = 7
}
