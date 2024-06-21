import os

import pandas as pd
import requests

from data_pipeline_tools.auth import hibob_headers
from data_pipeline_tools.util import write_to_bigquery

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


def get_employee_ids(config: dict[str:str]) -> list[str]:
    url = "https://api.hibob.com/v1/people/search"

    payload = {
        "fields": ["root.id"],
        "showInactive": False,
        "humanReadable": "REPLACE",
    }
    response = requests.post(url, headers=config["headers"], json=payload)
    return [employee["id"] for employee in response.json()["employees"]]


def get_employee_balance(employee_id: str, config: dict[str:str]) -> dict[str:str]:
    url = f"https://api.hibob.com/v1/timeoff/employees/{employee_id}/balance?policyType=TPXimpact%20Holiday&date=2024-12-31"
    response = requests.get(url, headers=config["headers"])
    return response.json()


def main(data: dict, context: dict = None):
    service = "Data Pipeline - HiBob Employees"
    config = load_config(project_id, service)
    balances = [
        get_employee_balance(employee_id, config)
        for employee_id in get_employee_ids(config)
    ]
    write_to_bigquery(
        config,
        pd.DataFrame([balance for balance in balances if balance]),
        "WRITE_TRUNCATE",
    )


if __name__ == "__main__":
    main({})
