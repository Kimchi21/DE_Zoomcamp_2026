variable "credentials" {
  description = "My Credentials"
  default     = "./keys/de-zoomcamp-485314-f87838d3d997.json"
}

variable "project" {
  description = "Project"
  default     = "de-zoomcamp-485314"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "google_bigquery_dataset" {
  description = "The BigQuery dataset name"
  default     = "example_dataset"
}

variable "google_storage_bucket_class" {
  description = "The Google Cloud Storage class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "de-zoomcamp-485314-test-bucket-2026"
}