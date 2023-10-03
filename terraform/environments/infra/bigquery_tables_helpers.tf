resource "google_bigquery_table" "months_columns" {
  dataset_id = google_bigquery_dataset.helper_tables.dataset_id
  table_id   = "months_columns"

  time_partitioning {
    type = "DAY"
  }

  labels = {
    env = var.env
  }

  deletion_protection = false

  encryption_configuration {
    kms_key_name = google_kms_crypto_key.bigquery_key.id
  }
}

