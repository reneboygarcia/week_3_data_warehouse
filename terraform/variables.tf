locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "service_account" {
  description = "hw1terraform-a5f48a64ab1e.json"
}

variable "project" {
  description = "hw1terraform"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/compute/docs/regions-zones"
  default = "us-central1"
  type = string
}

variable "zone" {
  description = "A zone is a deployment area within a region. Choose as per your location: https://cloud.google.com/compute/docs/regions-zones"
  default = "us-central1-c"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "trips_data_all"
}