# imports
from pathlib import Path
import urllib.request
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket, cloud_storage_download_blob_to_file
from prefect_gcp import GcpCredentials
from google.cloud import bigquery

print("Setup Complete")

# Download trip data from GCS
@task(log_prints=True, name="etl-gcs-to-bq")
def etl_gcs_to_bq(year: int, month: int):
    gcp_creds_block = GcpCredentials.load("prefect-gcs-2023-creds")
    gcp_creds = gcp_creds_block.get_credentials_from_service_account()
    client = bigquery.Client(credentials=gcp_creds)
    table_id = "dtc-de-2023.ny_taxi.ny_taxi_tripdata_2019"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        schema=[
            bigquery.SchemaField("dispatching_base_num", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pickup_datetime", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("dropOff_datetime", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("PUlocationID", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("DOlocationID", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("SR_Flag", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Affiliated_base_number", "STRING", mode="NULLABLE"),
        ],
    )
    uri = f"gs://ny_taxi_bucket_de_2023/2019/fhv_tripdata_{year}-{month:02}.parquet"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows.")


# Parent flow ETL
@flow(log_prints=True, name="etl-parent-to-bq")
def etl_parent_bq_flow(
    year: int = 2019, months: list[int] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
):
    for month in months:
        etl_gcs_to_bq(year, month)


# run main
if __name__ == "__main__":
    year = 2019
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]

    etl_parent_bq_flow(year, months)
