locals {
  dag_files = fileset("./dags/", "*.py")
  extract_files = fileset("./ETL/extract/", "*.py")
  transform_files = fileset("./ETL/transform/", "*.py")
  load_files = fileset("./ETL/load/", "**")
  ge_files = fileset("./src/great_expectation/", "**")
  schema_files = fileset("./src/schemas/", "*.json")

}

resource "google_storage_bucket" "data" {
  name          = "${var.project_id}-${var.bucket_suffix}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "scripts" {
  name          = "${var.project_id}-${var.bucket_suffix}-scripts"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

resource "google_storage_bucket_object" "dags" {
  for_each = local.dag_files

  bucket       = google_storage_bucket.scripts.name
  name         = "dags/${each.value}"
  source       = "./dags/${each.value}"
}

resource "google_storage_bucket_object" "extract" {
  for_each = local.extract_files

  bucket       = google_storage_bucket.scripts.name
  name         = "dags/ETL/extract/${each.value}"
  source       = "./ETL/extract/${each.value}"
}

resource "google_storage_bucket_object" "transform" {
  for_each = local.transform_files

  bucket       = google_storage_bucket.scripts.name
  name         = "dags/ETL/transform/${each.value}"
  source       = "./ETL/transform/${each.value}"
}

resource "google_storage_bucket_object" "load" {
  for_each = local.load_files

  bucket       = google_storage_bucket.scripts.name
  name         = "dags/ETL/load/${each.value}"
  source       = "./ETL/load/${each.value}"
}

resource "google_storage_bucket_object" "great_expectations" {
  for_each = local.ge_files

  bucket       = google_storage_bucket.scripts.name
  name         = "dags/src/great_expectation/${each.value}"
  source       = "./src/great_expectation/${each.value}"
}

resource "google_storage_bucket_object" "schemas" {
  for_each = local.schema_files

  bucket       = google_storage_bucket.scripts.name
  name         = "dags/src/schemas/${each.value}"
  source       = "./src/schemas/${each.value}"
}

