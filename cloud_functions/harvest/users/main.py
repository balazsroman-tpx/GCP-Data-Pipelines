import asyncio
import os

import requests

from data_pipeline_tools.headers import harvest_headers
from data_pipeline_tools.asyncs import get_all_data
from data_pipeline_tools.util import write_to_bigquery, flatten_columns


project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
if not project_id:
    project_id = input("Enter GCP project ID: ")


def load_config(project_id, service) -> dict:
    return {
        "url": "https://api.harvestapp.com/v2/users?page=",
        "headers": harvest_headers(project_id, service),
        "dataset_id": "Harvest",
        "gcp_project": project_id,
        "table_name": "users",
        "location": "EU",
        "service": service,
    }


def get_harvest_pages(url: str, headers: dict):
    url = f"{url}1"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json()

        return data["total_pages"], data["total_entries"]
    except (requests.exceptions.RequestException, KeyError) as e:
        print(f"Error retrieving total pages: {e}")
        return None


def main(data: dict, context):
    service = "Data Pipeline - Harvest User Project Assignments"
    config = load_config(project_id, service)

    pages, entries = get_harvest_pages(config["url"], config["headers"])
    print(f"Total pages: {pages}")
    df = asyncio.run(
        get_all_data(
            config["url"], config["headers"], pages, "users", batch_size=10
        )
    ).reset_index(drop=True)
    df = flatten_columns(df)

    assert len(df) == entries
    write_to_bigquery(config, df, "WRITE_TRUNCATE")


if __name__ == "__main__":
    main({}, None)
