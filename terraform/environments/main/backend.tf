terraform {

  backend "gcs" {
    bucket = "tpx-cheetah-data-pipelines-tf-state-prod"
    prefix = "terraform/state"
  }

}
