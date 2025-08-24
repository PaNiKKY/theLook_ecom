output "data_bucket" {
  value = google_storage_bucket.data.name
}

output "scripts_bucket" {
  value = google_storage_bucket.scripts.name
}
