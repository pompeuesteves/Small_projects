"""
Create in Big Query database:
 shape_2xs.equipment_sensors
 shape_2xs.equipment
 shape_2xs.equipment_failure_sensors

@created by: esteves
"""
from google.cloud import storage
import os
import pandas as pd
from io import BytesIO
from google.cloud import bigquery
import datetime


def get_bucket_csv(data):
    # stablish connection
    if os.environ.get('PRODUCTION'):
        client = storage.Client()
    else:
        client = storage.Client.from_service_account_json("../storage2bq.json")

    bucket = client.get_bucket(data["bucket"])
    blob = bucket.get_blob(data["name"])
    data_str = blob.download_as_string()
    string_df = BytesIO(data_str)
    df = pd.read_csv(string_df, sep=';')

    return df


def get_bucket_json(data):
    # stablish connection
    if os.environ.get('PRODUCTION'):
        client = storage.Client()
    else:
        client = storage.Client.from_service_account_json("../storage2bq.json")

    bucket = client.get_bucket(data["bucket"])
    blob = bucket.get_blob(data["name"])
    data_str = blob.download_as_string()
    df = pd.read_json(data_str, orient='records')

    return df


def get_bucket_log(data):
    # stablish connection
    if os.environ.get('PRODUCTION'):
        client = storage.Client()
    else:
        client = storage.Client.from_service_account_json("../storage2bq.json")

    bucket = client.get_bucket(data["bucket"])
    blob = bucket.get_blob(data["name"])
    data_str = blob.download_as_string()
    string_df = BytesIO(data_str)
    df = pd.read_csv(string_df, sep='\t', header=None)

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


def clean_log_data(df):
    # first step: data cleaning
    for index, row in df.iterrows():
        row[0] = row[0].replace("[", "").replace("]", "")
        row[2] = row[2].replace("[", " ").replace("]:", "")
        row[3] = row[3].replace("(", "")
        row[4] = row[4].replace(", ", " ")
        row[5] = row[5].replace(")", "")

    # second step: identify the columns
    df = df.rename(columns={0: "time", 1: "e_label", 2: "s", 3: "t_label", 4: "v", 5: "vibration"})
    df[['s_label', 'sensor_id']] = df.s.str.split(expand=True)
    df[['temperature', 'v_label']] = df.v.str.split(expand=True)

    # third step: delete non use columns
    df = df.drop(columns=['s', 'v', 'e_label', 't_label', 's_label', 'v_label'])

    # fourth step: cast to correct type
    df['sensor_id'] = df['sensor_id'].astype(int)
    df['vibration'] = df['vibration'].astype(float)
    df['temperature'] = df['temperature'].astype(float)
    df['time'] = pd.to_datetime(df['time'])

    return df


if __name__ == "__main__":
    start = datetime.datetime.now()
    data = dict()

    # get equipment_sensors
    data["bucket"] = "shape_2xs"
    data["name"] = "equipment_sensors.csv"
    df_s2e = get_bucket_csv(data)
    save_bigquery(df_s2e, 'equipment_sensors', data["bucket"])
    print(f"--- equipment_sensors.csv: {datetime.datetime.now() - start} seconds")

    # get equipment
    data["bucket"] = "shape_2xs"
    data["name"] = "equipment.json"
    df_e = get_bucket_json(data)
    save_bigquery(df_e, 'equipment', data["bucket"])
    print(f"--- equipment.json: {datetime.datetime.now() - start} seconds")

    # get sensors_status
    data["bucket"] = "shape_2xs"
    data["name"] = "equipment_failure_sensors.log"
    df_ss = get_bucket_log(data)
    df_ss = clean_log_data(df_ss)
    save_bigquery(df_ss, 'equipment_failure_sensors', data["bucket"])
    print(f"--- equipment_failure_sensors.log: {datetime.datetime.now() - start} seconds")

    print('Success!')

