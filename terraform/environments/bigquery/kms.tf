data "google_kms_key_ring" "dashboards_keyring" {
  name     = "dashboards_keyring"
  location = "europe-west2"
}

data "google_kms_crypto_key" "bigquery_key" {
  name     = "bigquery_key"
  key_ring = data.google_kms_key_ring.dashboards_keyring.id
}
data "google_kms_crypto_key" "pubsub_key" {
  name     = "pubsub_key"
  key_ring = data.google_kms_key_ring.dashboards_keyring.id
}

