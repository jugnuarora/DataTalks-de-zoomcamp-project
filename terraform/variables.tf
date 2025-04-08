variable "credentials" {
  description = "My Credentials"
  default     = "../secrets/gcp_credentials.json"
}

variable "project_id" {
  description = "Google Cloud Project ID"
  type        = string
}

variable "region" {
  description = "Google Cloud Region"
  type        = string
  default     = "europe-west1"
}

variable "bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
}

variable "service_account_email" {
  description = "Email of the service account that needs access to GCS"
  type        = string
}

variable "service_account_key_file" {
  description = "Path to the service account key file"
  type        = string
  default     = "../secrets/gcp_credentials.json"
}

variable "bq_dataset_name1" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "source_tables"
}

variable "bq_dataset_name2" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "external"
}