from google.cloud import storage
from google.cloud import bigquery
import os
import pandas as pd
import json
import re


def clean_html(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext


def single_quotes(raw_json):
    p = re.compile('(?<!\\\\)\'')
    str = p.sub('\"', raw_json)
    return str


def get_bucket_json(data):
    # stablish connection
    if os.environ.get('PRODUCTION'):
        client = storage.Client()
    else:
        client = storage.Client.from_service_account_json("../storage2bq.json")

    bucket = client.get_bucket(data["bucket"])
    blob = bucket.get_blob(data["name"])
    data_str = blob.download_as_string()

    # replace single quotes to double quotes
    data_str = single_quotes(str(data_str, 'utf-8'))

    # transform json to dataframe
    data_json = json.loads(data_str)
    elevations = json.dumps(data_json)
    df = pd.read_json(elevations)

    return df


def save_bigquery(df, table_name, dataset):
    # stablish connection
    if os.environ.get('PRODUCTION'):
        client = bigquery.Client()
    else:
        client = bigquery.Client.from_service_account_json("../storage2bq.json")

    table_ref = client.dataset(dataset).table(table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True

    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    # Waits for table load to complete.
    load_job.result()


def starwars_application_bigquery(param_data):
    starwars_data = get_bucket_json(param_data)
    save_bigquery(starwars_data, 'starwars_people', 'starwars')
    return "ok"


# For local tests
if __name__ == "__main__":
    # for debug purpose
    data = dict()
    data["bucket"] = "bucket_starwars"
    data["name"] = "people"
    starwars_application_bigquery(data)
