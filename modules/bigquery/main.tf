resource "google_bigquery_dataset" "this" {
  dataset_id = var.bq_dataset
  location   = var.region
  project    = var.project_id
}

locals {
  tables_list = jsondecode(file(var.tables_schema_file))
  tables_map  = { for t in local.tables_list : t.table_name => t }
}

resource "google_bigquery_table" "tables" {
  for_each   = local.tables_map
  dataset_id = google_bigquery_dataset.this.dataset_id
  table_id   = each.value.table_name

  schema = jsonencode(each.value.schema)

  dynamic "time_partitioning" {
    for_each = lookup(each.value, "partitioning", null) == null ? [] : [each.value.partitioning]
    content {
      type           = time_partitioning.value.type
      field          = lookup(time_partitioning.value, "field", null)
      require_partition_filter = lookup(time_partitioning.value, "require_filter", null)
    }
  }
}
