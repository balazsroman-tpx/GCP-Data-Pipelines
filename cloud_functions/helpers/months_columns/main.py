import os
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from data_pipeline_tools.util import write_to_bigquery

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


def main(data: dict, context):
    service = "Data Pipeline - Months Helper Table"
    config = load_config(project_id, service)

    df = get_financial_year_months_df()

    write_to_bigquery(config, df, "WRITE_TRUNCATE")

def get_financial_year_months_df():

    # current date
    now = datetime.now()

    # start of the current financial year
    if now.month < 4:
        start_date = datetime(now.year - 1, 4, 1)
    else:
        start_date = datetime(now.year, 4, 1)

    # end of the next financial year
    end_date = datetime(start_date.year + 2, 3, 31)

    # list to hold column names
    columns = []

    # generate column names for each month from the start of the current financial year to the end of the next financial year
    current_date = start_date
    while current_date <= end_date:
        columns.append(current_date.strftime('%Y-%m'))
        current_date += relativedelta(months=1)

    # create an empty DataFrame with the generated column names
    return pd.DataFrame(columns=columns)

if __name__ == "__main__":
    main({}, None)