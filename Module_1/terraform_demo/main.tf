terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
    credentials = ".\terraform_creds.json"
    project     = "terraform-demo-484316"
    region      = "europe-central2"
}