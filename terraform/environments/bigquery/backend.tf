terraform {

  backend "gcs" {
    bucket = "tpx-consulting-dashboards-bigquery-tf-state-prod"
    prefix = "terraform/state"
  }

}
