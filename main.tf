module "storage" {
  source             = "./modules/storage"
  project_id         = var.project_id
  region             = var.region
  bucket_suffix      = var.bucket_suffix
}

module "composer" {
  source                = "./modules/composer"
  project_id            = var.project_id
  region                = var.region
  composer_name         = var.composer_name
  service_account_name  = var.service_account_name
  bucket_suffix         = var.bucket_suffix
  source_dataset        = var.source_dataset
  bq_dataset            = var.bq_dataset
  composer_image_version = var.composer_image_version

  depends_on = [ module.storage ]
}

module "bigquery" {
  source            = "./modules/bigquery"
  project_id        = var.project_id
  bq_dataset        = var.bq_dataset
  region            = var.region
  tables_schema_file = var.tables_schema_file
}
