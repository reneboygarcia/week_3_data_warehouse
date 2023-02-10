terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
    credentials = "${file(var.service_account)}"
    project = var.project
    region = var.region
    zone = var.zone
}

resource "google_bigquery_dataset" "example_dataset" {
  dataset_id = "example_dataset"
}

resource "google_bigquery_table" "example_table" {
  dataset_id = google_bigquery_dataset.example_dataset.dataset_id
  table_id   = "example_table"

  schema = [
    {
      name = "column_1",
      type = "STRING",
    },
    {
      name = "column_2",
      type = "INTEGER",
    },
  ]
}

resource "google_storage_bucket" "example_bucket" {
  name = "example_bucket"
}

resource "google_storage_bucket_object" "example_object" {
  name   = "example_object"
  bucket = google_storage_bucket.example_bucket.name
  source = "path/to/example_data.csv"
}

resource "google_bigquery_load_job" "example_load_job" {
  dataset_id = google_bigquery_dataset.example_dataset.dataset_id
  table_id   = google_bigquery_table.example_table.table_id

  source_uris = [
    "gs://${google_storage_bucket.example_bucket.name}/${google_storage_bucket_object.example_object.name}"
  ]

  schema = [
    {
      name = "column_1",
      type = "STRING",
    },
    {
      name = "column_2",
      type = "INTEGER",
    },
  ]

  write_disposition = "WRITE_TRUNCATE"
}

resource "google_cloud_scheduler_job" "example_scheduler_job" {
  name     = "example_scheduler_job"
  schedule = "0 0 * * *"

  target {
    http_target {
      http_method = "POST"
      uri         = "https://example.cloudfunctions.net/load_data"
    }
  }
}