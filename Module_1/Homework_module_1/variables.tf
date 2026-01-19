variable "project" {
  default = "terraform-homework-484821"
}

variable "location" {
  description = "Location to be used when creating resources"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset Name"
  default     = "homework_dataset"
}

variable "gcp_bucket_name" {
  description = "My Storage bucket name"
  default     = "terraform-homework-484821-terra-bucket"
}

variable "gcp_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "credentials" {
  description = "My credentials file"
  default = "./terraform_creds.json"
}