from google.cloud import storage
from google.cloud import bigquery
import json
import os


def get_bucket(param_data):
    if os.environ.get('PRODUCTION'):
        client = storage.Client()
    else:
        client = storage.Client.from_service_account_json("../storage2bq.json")
    # Get the file that has been uploaded to GCS
    bucket = client.get_bucket(param_data["bucket"])
    blob = bucket.get_blob(param_data["name"])
    data_str = blob.download_as_string()
    return data_str


def save_bigquery(table):
    """Import a file into BigQuery"""
    if os.environ.get('PRODUCTION'):
        client = bigquery.Client()
    else:
        client = bigquery.Client.from_service_account_json("../storage2bq.json")

    table_ref = client.dataset("zTeste1").table("starwars1")

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    table = json.loads(table)
    load_job = client.load_table_from_json(table, table_ref, job_config=job_config)

    # Waits for table load to complete.
    load_job.result()


def starwars_application_bigquery(param_data):
    starwars_data = get_bucket(param_data)
    print(starwars_data)
    save_bigquery(starwars_data)
    return "ok"


# For local tests
if __name__ == "__main__":
    # for debug purpose
    data = dict()
    data["bucket"] = "bucket_teste_esteves_01"
    data["name"] = "people"
    starwars_application_bigquery(data)
