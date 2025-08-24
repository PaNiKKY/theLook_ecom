variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Region for resources"
  type        = string
}

variable "service_account_name" {
  description = "Service account name for Composer"
  type        = string
  
}

variable "composer_image_version" {
  description = "Composer image version"
  type        = string
}

variable "bucket_suffix" {
  description = "Suffix for bucket names to ensure uniqueness"
  type        = string
}

variable "composer_name" {
  description = "Composer environment name"
  type        = string
}

variable "bq_dataset" {
  description = "BigQuery dataset name for Composer"
  type        = string
}

variable "source_dataset" {
  description = "BigQuery dataset name for source data"
  type        = string
}

variable "tables_schema_file" {
  description = "Path to JSON file with all table schemas"
  type        = string
}
