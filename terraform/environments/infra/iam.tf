resource "google_project_iam_member" "secret_accessor" {
  project = var.project
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${var.project}@appspot.gserviceaccount.com"
}
resource "google_project_iam_member" "cryptoKeyEncrypterDecrypter" {
  project = var.project
  role    = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member  = "serviceAccount:${var.project}@appspot.gserviceaccount.com"
}
