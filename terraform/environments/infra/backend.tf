terraform {

  backend "gcs" {
    bucket = "tpx-consulting-dashboards-data-pipelines-tf-state-prod"
    prefix = "terraform/state"
  }

}
