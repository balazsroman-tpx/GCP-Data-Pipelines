resource "google_bigquery_table" "time_off" {
  dataset_id = google_bigquery_dataset.hibob_raw.dataset_id
  table_id   = "time_off"

  time_partitioning {
    type = "DAY"
  }

  labels = {
    env = var.env
  }

  deletion_protection = false

  encryption_configuration {
    kms_key_name = data.google_kms_crypto_key.bigquery_key.id
  }
}

