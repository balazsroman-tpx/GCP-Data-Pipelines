data "google_kms_key_ring" "bigquery_key_ring" {
  name     = "bigquery_key-ring"
  location = "europe-west2"
}

data "google_kms_crypto_key" "bigquery_key" {
  name     = "bigquery"
  key_ring = data.google_kms_key_ring.bigquery_key_ring.id
}
data "google_iam_policy" "bigquery_key_encrypt_decrypt" {
  binding {
    role = "roles/cloudkms.cryptoKeyVersions.useToEncrypt"
    members = [
      "serviceAccount:bq-${data.google_project.project_number.number}@bigquery-encryption.iam.gserviceaccount.com",
    ]
  }
}
resource "google_bigquery_dataset" "harvest_raw" {
  dataset_id  = "Harvest_Raw"
  description = "Dataset for tables containing raw harvest data"
  location    = "europe-west2"

  labels = {
    env = var.env
  }
  default_encryption_configuration {
    kms_key_name = data.google_kms_crypto_key.bigquery_key.id
  }
}


