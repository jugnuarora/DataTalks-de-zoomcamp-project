output "bucket_name" {
  description = "The name of the created GCS bucket"
  value       = google_storage_bucket.bucket.name
}

output "project_id" {
  description = "The GCP project ID"
  value       = var.project_id
}

output "GCP_CREDS" {
  value = file("${path.module}/../secrets/gcp_credentials.json")
  description = "The gcp credentials json file."
}