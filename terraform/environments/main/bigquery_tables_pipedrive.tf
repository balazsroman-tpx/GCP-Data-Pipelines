resource "google_bigquery_table" "deals" {
  dataset_id = google_bigquery_dataset.pipedrive_raw.dataset_id
  table_id   = "deals"

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


resource "google_bigquery_table" "organisations" {
  dataset_id = google_bigquery_dataset.pipedrive_raw.dataset_id
  table_id   = "organisations"

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
