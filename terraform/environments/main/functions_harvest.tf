# Generates an archive of the source code compressed as a .zip file.
data "archive_file" "harvest_timesheet" {
  type        = "zip"
  source_dir  = "../../../cloud_functions/harvest/timesheet"
  output_path = "/tmp/harvest_timesheet.zip"
}

# Add source code zip to the Cloud Function's bucket
resource "google_storage_bucket_object" "harvest_timesheet" {
  source       = data.archive_file.harvest_timesheet.output_path
  content_type = "application/zip"

  # Append to the MD5 checksum of the files's content
  # to force the zip to be updated as soon as a change occurs
  name   = "cloud_function-${data.archive_file.harvest_timesheet.output_md5}.zip"
  bucket = data.google_storage_bucket.function_bucket.name
}

resource "google_cloudfunctions_function" "harvest_timesheet" {
  name                = "harvest_timesheet_pipe"
  runtime             = "python39" # of course changeable
  available_memory_mb = 2048
  timeout             = 540
  # Get the source code of the cloud function as a Zip compression
  source_archive_bucket = data.google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.harvest_timesheet.name

  # Must match the function name in the cloud function `main.py` source code
  entry_point                  = "main"
  https_trigger_security_level = "SECURE_ALWAYS"
  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.cloud_function_trigger_hot.id
  }

  environment_variables = {
    "DATASET_ID"           = google_bigquery_dataset.harvest_raw.dataset_id
    "TABLE_NAME"           = google_bigquery_table.timesheets.table_id
    "TABLE_LOCATION"       = google_bigquery_dataset.harvest_raw.location
    "GOOGLE_CLOUD_PROJECT" = var.project

  }
}


# Generates an archive of the source code compressed as a .zip file.
data "archive_file" "harvest_users" {
  type        = "zip"
  source_dir  = "../../../cloud_functions/harvest/users"
  output_path = "/tmp/harvest_users.zip"
}

# Add source code zip to the Cloud Function's bucket
resource "google_storage_bucket_object" "harvest_users" {
  source       = data.archive_file.harvest_users.output_path
  content_type = "application/zip"

  # Append to the MD5 checksum of the files's content
  # to force the zip to be updated as soon as a change occurs
  name   = "cloud_function-${data.archive_file.harvest_users.output_md5}.zip"
  bucket = data.google_storage_bucket.function_bucket.name
}

resource "google_cloudfunctions_function" "harvest_users" {
  name                = "harvest_users_pipe"
  runtime             = "python39" # of course changeable
  available_memory_mb = 1024
  timeout             = 540
  # Get the source code of the cloud function as a Zip compression
  source_archive_bucket = data.google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.harvest_users.name

  # Must match the function name in the cloud function `main.py` source code
  entry_point                  = "main"
  https_trigger_security_level = "SECURE_ALWAYS"
  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.cloud_function_trigger_cold.id
  }

  environment_variables = {
    "DATASET_ID"           = google_bigquery_dataset.harvest_raw.dataset_id
    "TABLE_NAME"           = google_bigquery_table.users.table_id
    "TABLE_LOCATION"       = google_bigquery_dataset.harvest_raw.location
    "GOOGLE_CLOUD_PROJECT" = var.project

  }
}


# Generates an archive of the source code compressed as a .zip file.
data "archive_file" "harvest_user_project_assignments" {
  type        = "zip"
  source_dir  = "../../../cloud_functions/harvest/user_project_assignments"
  output_path = "/tmp/harvest_user_project_assignments.zip"
}

# Add source code zip to the Cloud Function's bucket
resource "google_storage_bucket_object" "harvest_user_project_assignments" {
  source       = data.archive_file.harvest_user_project_assignments.output_path
  content_type = "application/zip"

  # Append to the MD5 checksum of the files's content
  # to force the zip to be updated as soon as a change occurs
  name   = "cloud_function-${data.archive_file.harvest_user_project_assignments.output_md5}.zip"
  bucket = data.google_storage_bucket.function_bucket.name
}

resource "google_cloudfunctions_function" "harvest_user_project_assignments" {
  name                = "harvest_user_project_assignments_pipe"
  runtime             = "python39" # of course changeable
  available_memory_mb = 1024
  timeout             = 540
  # Get the source code of the cloud function as a Zip compression
  source_archive_bucket = data.google_storage_bucket.function_bucket.name
  source_archive_object = google_storage_bucket_object.harvest_user_project_assignments.name

  # Must match the function name in the cloud function `main.py` source code
  entry_point                  = "main"
  https_trigger_security_level = "SECURE_ALWAYS"
  event_trigger {
    event_type = "providers/cloud.pubsub/eventTypes/topic.publish"
    resource   = google_pubsub_topic.cloud_function_trigger_cold.id
  }

  environment_variables = {
    "DATASET_ID"           = google_bigquery_dataset.harvest_raw.dataset_id
    "TABLE_NAME"           = google_bigquery_table.user_project_assignments.table_id
    "TABLE_LOCATION"       = google_bigquery_dataset.harvest_raw.location
    "GOOGLE_CLOUD_PROJECT" = var.project

  }
}
