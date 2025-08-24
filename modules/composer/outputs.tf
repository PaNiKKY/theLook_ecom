output "composer_name" {
  value = google_composer_environment.this.name
}

output "composer_airflow_uri" {
  value = google_composer_environment.this.config[0].airflow_uri
}
