import os

import pandas as pd

from data_pipeline_tools.forecast_tools import forecast_client
from data_pipeline_tools.util import write_to_bigquery

project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
if not project_id:
    project_id = "tpx-consulting-dashboards"


def load_config(project_id, service) -> dict:
    return {
        "dataset_id": os.environ.get("DATASET_ID"),
        "gcp_project": project_id,
        "table_name": os.environ.get("TABLE_NAME"),
        "location": os.environ.get("TABLE_LOCATION"),
        "service": service,
    }


def main(data: dict, context: dict = None):
    service = "Data Pipeline - Forecast Clients"
    config = load_config(project_id, service)
    client = forecast_client(project_id)
    write_to_bigquery(
        config,
        pd.DataFrame([client._json_data for client in client.get_clients()]),
        "WRITE_TRUNCATE",
    )
    print("Done")


if __name__ == "__main__":
    main({})
