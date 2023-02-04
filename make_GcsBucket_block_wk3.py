from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# Load GCP Credentials block 
gcp_cred_block = GcpCredentials.load("ny-taxi-gcp-creds")

# Define the GcsBucket 

gcs_bucket = GcsBucket(bucket="ny_taxi_bucket_dtc", 
    gcp_credentials=gcp_cred_block)

gcs_bucket.save("ny-taxi-bucket", overwrite=True)
