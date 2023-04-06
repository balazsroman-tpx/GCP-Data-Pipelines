import asyncio
import os

from data_pipeline_tools.asyncs import get_all_data
from data_pipeline_tools.auth import harvest_headers
from data_pipeline_tools.util import (
    find_and_flatten_columns,
    get_harvest_pages,
    write_to_bigquery,
)

project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
if not project_id:
    project_id = input("Enter GCP project ID: ")


def load_config(project_id, service) -> dict:
    return {
        "url": "https://api.harvestapp.com/v2/expenses?page=",
        "headers": harvest_headers(project_id, service),
        "dataset_id": os.environ.get("DATASET_ID"),
        "gcp_project": project_id,
        "table_name": os.environ.get("TABLE_NAME"),
        "location": os.environ.get("TABLE_LOCATION"),
        "service": service,
    }


def main(data: dict, context):
    service = "Data Pipeline - Harvest Expenses"
    config = load_config(project_id, service)

    pages, entries = get_harvest_pages(config["url"], config["headers"])

    print(f"Total pages: {pages}")
    df = asyncio.run(
        get_all_data(config["url"], config["headers"], pages, "expenses", batch_size=10)
    ).reset_index(drop=True)
    df = find_and_flatten_columns(df)

    assert len(df) == entries
    write_to_bigquery(config, df, "WRITE_TRUNCATE")


if __name__ == "__main__":
    main({}, None)
