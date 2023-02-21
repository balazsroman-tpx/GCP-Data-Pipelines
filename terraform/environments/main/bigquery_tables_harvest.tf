resource "google_bigquery_table" "timesheets" {
  dataset_id = google_bigquery_dataset.harvest_raw.dataset_id
  table_id   = "timesheets"

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

resource "google_bigquery_table" "users" {
  dataset_id = google_bigquery_dataset.harvest_raw.dataset_id
  table_id   = "users"

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

resource "google_bigquery_table" "user_project_assignments" {
  dataset_id = google_bigquery_dataset.harvest_raw.dataset_id
  table_id   = "user_project_assignments"

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
