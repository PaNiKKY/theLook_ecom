output "data_bucket" {
  value = module.storage.data_bucket
}

output "scripts_bucket" {
  value = module.storage.scripts_bucket
}

output "composer_name" {
  value = module.composer.composer_name
}

output "composer_airflow_uri" {
  value = module.composer.composer_airflow_uri
}

output "bq_dataset" {
  value = module.bigquery.dataset_id
}

output "bq_tables" {
  value = module.bigquery.tables
}
