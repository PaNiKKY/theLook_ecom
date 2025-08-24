output "dataset_id" {
  value = google_bigquery_dataset.this.dataset_id
}

output "tables" {
  value = [for t in google_bigquery_table.tables : t.table_id]
}
