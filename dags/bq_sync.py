"""
BigQuery External Iceberg Table sync helper.

After each Silver Spark job runs, Iceberg writes a new metadata snapshot.
BigQuery External Tables need to be pointed at the latest metadata file.
This module handles that automatically by:
  1. Reading version-hint.text from the Iceberg metadata folder
  2. Constructing the correct vN.metadata.json URI
  3. Creating or replacing the BigQuery External Table

Required Airflow Variables:
    BQ_PROJECT   — GCP project ID, e.g. "highlands-lakehouse"
    BQ_DATASET   — BigQuery dataset for external tables, e.g. "silver_ext"
    BQ_LOCATION  — BigQuery dataset location, e.g. "asia-southeast1"
    BQ_CONNECTION — BigLake connection resource name,
                    e.g. "projects/highlands-lakehouse/locations/asia-southeast1/connections/highlands-biglake"
"""

from airflow.models import Variable


def _make_credentials(keyfile: str):
    from google.oauth2 import service_account
    scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
    ]
    return service_account.Credentials.from_service_account_file(keyfile, scopes=scopes)


def _latest_metadata_uri(bucket_name: str, iceberg_table_prefix: str, keyfile: str) -> str:
    """
    Read version-hint.text from the Iceberg metadata folder and return
    the URI of the current metadata JSON file.

    Iceberg Hadoop catalog writes:
        metadata/version-hint.text  → contains integer N (current snapshot)
        metadata/vN.metadata.json   → full table definition for that snapshot
    """
    from google.cloud import storage
    creds     = _make_credentials(keyfile)
    gcs       = storage.Client(credentials=creds)
    hint_blob = gcs.bucket(bucket_name).blob(
        f"{iceberg_table_prefix}/metadata/version-hint.text"
    )
    version = int(hint_blob.download_as_text().strip())
    return (
        f"gs://{bucket_name}/{iceberg_table_prefix}"
        f"/metadata/v{version}.metadata.json"
    )


def sync_bq_external_table(
    bucket: str,
    iceberg_prefix: str,
    bq_table: str,
    keyfile: str,
) -> None:
    """
    Create or replace a BigQuery External Iceberg Table pointing to the
    latest Iceberg snapshot.

    Args:
        bucket:         GCS bucket name (no gs:// prefix)
        iceberg_prefix: Path inside bucket, e.g. "iceberg/silver/weather"
        bq_table:       BigQuery table name (without dataset), e.g. "weather"
        keyfile:        Absolute path to GCP SA JSON keyfile inside container
    """
    project    = Variable.get("BQ_PROJECT")
    dataset    = Variable.get("BQ_DATASET")
    location   = Variable.get("BQ_LOCATION")
    connection = Variable.get("BQ_CONNECTION")

    metadata_uri  = _latest_metadata_uri(bucket, iceberg_prefix, keyfile)
    full_table_id = f"{project}.{dataset}.{bq_table}"

    ddl = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{full_table_id}`
        WITH CONNECTION `{connection}`
        OPTIONS (
            format = 'ICEBERG',
            uris   = ['{metadata_uri}']
        )
    """

    creds = _make_credentials(keyfile)
    from google.cloud import bigquery
    bq    = bigquery.Client(project=project, location=location, credentials=creds)
    bq.query(ddl).result()
    print(f"[bq_sync] Synced {full_table_id} → {metadata_uri}")
