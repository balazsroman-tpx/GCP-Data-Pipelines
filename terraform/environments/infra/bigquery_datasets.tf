
resource "google_bigquery_dataset" "harvest_raw" {
  dataset_id  = "Harvest_Raw"
  description = "Dataset for tables containing raw harvest data"
  location    = "europe-west2"

  labels = {
    env = var.env
  }
  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.bigquery_key.id
  }
}

resource "google_bigquery_dataset" "pipedrive_raw" {
  dataset_id  = "Pipedrive_Raw"
  description = "Dataset for tables containing raw pipedrive data"
  location    = "europe-west2"

  labels = {
    env = var.env
  }
  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.bigquery_key.id
  }
}

resource "google_bigquery_dataset" "pipedrive_processed" {
  dataset_id  = "Pipedrive_Processed"
  description = "Dataset for tables containing processed pipedrive data"
  location    = "europe-west2"

  labels = {
    env = var.env
  }
  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.bigquery_key.id
  }
}

resource "google_bigquery_dataset" "forecast_raw" {
  dataset_id  = "Forecast_Raw"
  description = "Dataset for tables containing raw forecast data"
  location    = "europe-west2"

  labels = {
    env = var.env
  }
  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.bigquery_key.id
  }
}



resource "google_bigquery_dataset" "hibob_raw" {
  dataset_id  = "hibob_raw"
  description = "Dataset for hibob raw data"
  location    = "europe-west2"

  labels = {
    env = var.env
  }
  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.bigquery_key.id
  }
}

resource "google_bigquery_dataset" "hibob_processed" {
  dataset_id  = "Hibob_Processed"
  description = "Dataset for tables containing processed hibob data"
  location    = "europe-west2"

  labels = {
    env = var.env
  }
  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.bigquery_key.id
  }
}


resource "google_bigquery_dataset" "helper_tables" {
  # Work out what this is for
  dataset_id  = "Helpers"
  description = "Dataset for helper tables"
  location    = "europe-west2"

  labels = {
    env = var.env
  }
  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.bigquery_key.id
  }
}

resource "google_bigquery_dataset" "invoices" {
  # Work out what this is for
  dataset_id  = "Variable_Data_Input"
  description = "Dataset for google sheets input"
  location    = "europe-west2"

  labels = {
    env = var.env
  }
  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.bigquery_key.id
  }
}
