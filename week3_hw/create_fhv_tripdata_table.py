# imports
from prefect_gcp.cloud_storage import cloud_storage_download_blob_to_file
from prefect_gcp import GcpCredentials
from google.cloud import bigquery
import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account


project = os.getenv("PROJECT_ID")
table_id = "dashlabs.lab_vmc.clients"

# Load GCP credentials
gcp_creds_block = GcpCredentials.load("prefect-gcs-2023-creds")
gcp_creds = gcp_creds_block.get_credentials_from_service_account()
client = bigquery.Client(credentials=gcp_creds)
table_id = "dtc-de-2023.ny_taxi.ny_taxi_tripdata_2019"

#  Define the schema
schema = [
    bigquery.SchemaField("dispatching_base_num", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("dropOff_datetime", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("PUlocationID", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("DOlocationID", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField(
        "SR_Flag",
        "FLOAT",
        mode="NULLABLE",
    ),
    bigquery.SchemaField("Affiliated_base_number", "STRING", mode="NULLABLE"),
]


# Create a table
table = bigquery.Table(table_id, schema=schema)
# Make an API request.
table = client.create_table(table)
print(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
