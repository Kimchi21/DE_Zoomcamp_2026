terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "test-bucket" {
  name                        = var.gcs_bucket_name
  location                    = var.location
  force_destroy               = true
  storage_class               = var.google_storage_bucket_class
  uniform_bucket_level_access = "true"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

}

resource "google_bigquery_dataset" "example_dataset" {
  dataset_id = var.google_bigquery_dataset
  location   = var.region
}