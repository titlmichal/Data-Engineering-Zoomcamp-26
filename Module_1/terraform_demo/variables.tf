variable "project" {
  default = "terraform-demo-484316"
}

variable "location" {
  description = "Location to be used when creating resources"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset Name"
  default     = "demo_dataset"
}

variable "gcp_bucket_name" {
  description = "My Storage bucket name"
  default     = "terraform-demo-484316-terra-bucket"
}

variable "gcp_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "credentials" {
  description = "My credentials file"
  default = "./terraform_creds.json"
}