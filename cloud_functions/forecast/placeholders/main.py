import pandas as pd
import os

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
    service = "Data Pipeline - Forecast Placeholders"
    config = load_config(project_id, service)
    client = forecast_client(project_id)
    placeholders_resp = unwrap_forecast_response(client.get_placeholders())

    placeholders_df = pd.DataFrame(placeholders_resp)

    write_to_bigquery(config, placeholders_df, "WRITE_TRUNCATE")
    print("Done")





if __name__ == "__main__":
    main({})
