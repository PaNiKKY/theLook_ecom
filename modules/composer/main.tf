resource "google_composer_environment" "this" {
  name     = var.composer_name
  region   = var.region

  storage_config {
    bucket = "${var.project_id}-${var.bucket_suffix}-scripts"
  }

  config {
    software_config {
      image_version = var.composer_image_version
      pypi_packages = {
        python-dotenv = "==1.1.1"
        great_expectations = "==0.18.22"
        pyarrow = "==14.0.0"
      }
      env_variables = {
          project_id = var.project_id
          bucket_suffix = var.bucket_suffix
          region = var.region
          source_dataset = var.source_dataset
          bq_dataset = var.bq_dataset
        }
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      triggerer {
        cpu        = 0.5
        memory_gb  = 1
        count      = 1
      }
      dag_processor {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu = 1
        memory_gb  = 8
        storage_gb = 1
        min_count  = 1
        max_count  = 3
      }
    }

    node_config {
      service_account = var.service_account_name
    }
  } 
}

# resource "google_project_iam_member" "composer-worker" {
#   project = var.project_id
#   role    = "roles/composer.worker"
#   member  = "serviceAccount:${var.service_account_name}"
# }
