import utils.variables as var

from datetime import datetime, timedelta
import random
import json
from faker import Faker
import pandas as pd
import requests
from google.cloud import storage
from google.cloud import secretmanager
from google.cloud import bigquery


def _parse_date(process_date_start):
    return datetime.strptime(process_date_start,"%Y-%m-%d")

def _plus_day(process_date_start):
    return process_date_start + timedelta(days=1)

def _format_date(process_date_start):
    return process_date_start.strftime('%Y%m%d')

def _format_date_short(process_date_start):
    return process_date_start.strftime('%Y-%m-%d')

def _extract_sales(process_date):
    rows = []
    fake = Faker()
    quantity = random.randint(1,10000)

    for _ in range(quantity):
        cantidad = random.randint(1,30)
        precioUni = round(random.uniform(10, 1000), 2)
        payload = {
            "id" : fake.random_number(digits=10),
            "product" : random.randint(1,500), 
            "quantity" :cantidad, 
            "unit_price" : precioUni,
            "tot_price" : round(cantidad * precioUni,2),
            "email_client" : fake.ascii_free_email(),
            "address" : fake.street_address(),
            "issue_date" : _format_date(process_date)
        } 
        rows.append(payload)       

    return json.dumps(rows)

def _fetch_secrets(secret_name):
    try:
        secret_client = secretmanager.SecretManagerServiceClient()
        secret = f'projects/{var.GCP_PROJECT}/secrets/{secret_name}/versions/{var.ALIAS}'
        secret_response = secret_client.access_secret_version(name=secret)
        secret_value = secret_response.payload.data.decode('UTF-8')
        return secret_value

    except Exception as e:
        print(f"Error accessing the secret: {e}")

def _upload_json_to_gcs(bucket_name, today_big, json_data, file_name):
    file_name_path = f'{file_name}.json'
    blob = f'sales/ingested/{today_big}/{file_name_path}'
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    destination_blob = bucket.blob(blob)
    destination_blob.upload_from_string(json_data)

def _read_json(response):
    return json.loads(response)

def _read_from_gcs(path):
    return pd.read_json(path)

def _extract_currency(process_date_str):
    url_currency = f'https://api.apis.net.pe/v1/tipo-cambio-sunat?fecha={process_date_str}'  
    response = requests.get(url_currency).json()
    return json.dumps([response])

def _transforming_sales(df_sale, df_currency):
    venta_usd = df_currency.loc[0, 'venta']
    df_sale['issue_date'] = pd.to_datetime(df_sale['issue_date'], format='%Y%m%d')
    df_sale['unit_price_USD'] = (df_sale['unit_price'] / venta_usd).round(2)
    df_sale['tot_price_USD'] = (df_sale['tot_price'] / venta_usd).round(2)
    df_sale['currency'] = 'USD'
    df_sale['currency_value'] = venta_usd
    df_sale['process_datetime'] = datetime.utcnow()
    return df_sale

def _append_to_bigquery_table(dataset, table, dataframe):
    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(dataset).table(table)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.job.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.job.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True)  

    job = bigquery_client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
    job.result()

def current_date():
    return datetime.utcnow().strftime('%Y-%m-%d')