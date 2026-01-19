terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = var.credentials
  project     = var.project
  region      = "europe-central2"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcp_bucket_name
  location      = var.location
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo-dataset" {
  dataset_id                 = var.bq_dataset_name
  location                   = var.location
  delete_contents_on_destroy = true
}