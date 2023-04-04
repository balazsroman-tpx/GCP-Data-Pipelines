data "google_kms_key_ring" "pub_sub_key_ring" {
  name     = "forecast_sheets-pub_sub_key-ring"
  location = "europe-west2"
}

data "google_kms_crypto_key" "pub_sub_key" {
  name     = "forecast_sheets-pub_sub"
  key_ring = data.google_kms_key_ring.pub_sub_key_ring.id
}

resource "google_pubsub_topic" "cloud_function_trigger_cold" {
  name         = "cloud-function-trigger-cold"
  kms_key_name = data.google_kms_crypto_key.pub_sub_key.id

}

resource "google_cloud_scheduler_job" "daily-5-45" {
  name        = "daily-5-45-trigger"
  description = "Scheduled daily to trigger cloud function at 5:45"
  schedule    = "*/45 5 * * *"

  pubsub_target {
    # topic.id is the topic's full resource name.
    topic_name = google_pubsub_topic.cloud_function_trigger_cold.id
    data       = base64encode("weekly")
  }
}

resource "google_pubsub_topic" "cloud_function_trigger_hot" {
  name         = "cloud-function-trigger-hot"
  kms_key_name = data.google_kms_crypto_key.pub_sub_key.id
}

resource "google_cloud_scheduler_job" "two-hourly-00" {
  name        = "every-two-hours-00"
  description = "Scheduled to trigger cloud function every two hours on the hour"
  schedule    = "0 */2 * * *"

  pubsub_target {
    # topic.id is the topic's full resource name.
    topic_name = google_pubsub_topic.cloud_function_trigger_hot.id
    data       = base64encode("2hr")
  }
}

resource "google_pubsub_topic" "cloud_function_trigger_hot_2" {
  name         = "cloud-function-trigger-hot-2"
  kms_key_name = data.google_kms_crypto_key.pub_sub_key.id
}

resource "google_cloud_scheduler_job" "two-hourly-15" {
  name        = "every-two-hours-15"
  description = "Scheduled to trigger cloud function every two hours 15 minutes past each hour"
  schedule    = "15 */2 * * *"

  pubsub_target {
    # topic.id is the topic's full resource name.
    topic_name = google_pubsub_topic.cloud_function_trigger_hot_2.id
    data       = base64encode("2hr15")
  }
}
