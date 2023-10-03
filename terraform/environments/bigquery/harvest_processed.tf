
resource "google_bigquery_dataset" "harvest_processed" {
  dataset_id  = "Harvest_Processed"
  description = "Dataset for tables containing processed harvest data"
  location    = "europe-west2"

  labels = {
    env = var.env
  }
  default_encryption_configuration {
    kms_key_name = data.google_kms_crypto_key.bigquery_key.id
  }
}


module "bigquery_scheduled_queries" {
  source     = "terraform-google-modules/bigquery/google//modules/scheduled_queries"
  version    = "6.1.1"
  project_id = var.project
  queries = [
    {
      name                   = "harvest_processed"
      location               = "europe-west2"
      data_source_id         = "scheduled_query"
      destination_dataset_id = google_bigquery_dataset.harvest_processed.dataset_id
      params = {
        destination_table_name_template = "harvest_processed"
        write_disposition               = "WRITE_TRUNCATE"
        query                           = file("${path.module}/queries/harvest_processed_master.sql")
      }
    }
  ]
}
