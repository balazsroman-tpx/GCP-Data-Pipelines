import pandas as pd
import os
from datetime import datetime

from data_pipeline_tools.forecast_tools import forecast_client
from data_pipeline_tools.util import unwrap_forecast_response, write_to_bigquery


project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
if not project_id:
    project_id = input("Enter GCP project ID: ")


def load_config(project_id, service) -> dict:
    return {
        "dataset_id": os.environ.get("DATASET_ID"),
        "gcp_project": project_id,
        "table_name": os.environ.get("TABLE_NAME"),
        "location": os.environ.get("TABLE_LOCATION"),
        "service": service,
    }


def main(data: dict, context: dict = None):
    service = "Data Pipeline - Forecast Projects"
    config = load_config(project_id, service)
    client = forecast_client(project_id)
    projects_resp = unwrap_forecast_response(client.get_projects())

    projects_df = pd.DataFrame(projects_resp)

    artificial_projects_df = get_artificial_projects()
    final_df = pd.concat([projects_df, artificial_projects_df], ignore_index=True)

    columns_to_drop = []
    final_df = final_df.drop(columns=columns_to_drop, errors="ignore")

    write_to_bigquery(config, final_df, "WRITE_TRUNCATE")
    print("Done")


def get_artificial_projects():
    return pd.DataFrame(
        [
            {
                "id": 999999,
                "name": "blank",
                "color": "white",
                "code": None,
                "notes": None,
                "start_date": None,
                "end_date": None,
                "harvest_id": None,
                "archived": False,
                "updated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "updated_by_id": None,
                "client_id": None,
                "tags": [],
            }
        ]
    )


if __name__ == "__main__":
    main({})
