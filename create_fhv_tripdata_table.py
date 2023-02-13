# imports
from prefect_gcp import GcpCredentials
from google.cloud import bigquery


# Load GCP credentials
def get_bigquery_client():
    gcp_creds_block = GcpCredentials.load("prefect-gcs-2023-creds")
    gcp_creds = gcp_creds_block.get_credentials_from_service_account()
    client = bigquery.Client(credentials=gcp_creds)
    return client


# Create a blank table on BigQuery
def create_fhv_tripdata_table(year: int = 2020):

    # get BQ client
    client = get_bigquery_client()

    # save to this table
    table_id = f"dtc-de-2023.ny_taxi.ny_taxi_tripdata_{year}"

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


if __name__ == "__main__":
    year = 2020
    # run this
    create_fhv_tripdata_table(year)
