import json
import os
import httpx
import pandas as pd
from data_pipeline_tools.util import write_to_bigquery
from data_pipeline_tools.auth import hibob_headers
import requests

project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
if not project_id:
    project_id = input("Enter GCP project ID: ")


def load_config(project_id, service) -> dict:
    return {
        "table_name": os.environ.get("TABLE_NAME"),
        "dataset_id": os.environ.get("DATASET_ID"),
        "location": os.environ.get("TABLE_LOCATION"),
        "headers": hibob_headers(project_id, service),
    }


def main(data: dict, context: dict = None):
    service = "Data Pipeline - HiBob People"
    config = load_config(project_id, service)
    client = httpx.Client()
    df = get_people(config, client)
    write_to_bigquery(config, df, "WRITE_TRUNCATE")


def get_people(config: dict, client: httpx.Client) -> pd.DataFrame:
    url = "https://api.hibob.com/v1/people/search"

    r = client.post(url, headers=config["headers"], timeout=None, json={})
    
    resp = json.loads(r.text)


if __name__ == "__main__":
    main({})
