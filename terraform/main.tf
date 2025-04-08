# Create a GCS bucket
resource "google_storage_bucket" "bucket" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true # Allows deletion even with objects present
#  depends_on    = [google_project_service.storage]


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# Create BigQuery datasets
resource "google_bigquery_dataset" "source_tables" {
  dataset_id = var.bq_dataset_name1
  location   = var.region
#  depends_on = [google_project_service.bigquery]
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "external" {
  dataset_id = var.bq_dataset_name2
  location   = var.region
#  depends_on = [google_project_service.bigquery]
  delete_contents_on_destroy = true
}