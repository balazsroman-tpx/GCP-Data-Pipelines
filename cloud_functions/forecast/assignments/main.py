import copy
import os
from datetime import datetime, timedelta

import pandas as pd

from data_pipeline_tools.forecast_tools import forecast_client
from data_pipeline_tools.util import unwrap_forecast_response, write_to_bigquery

START_DATE = datetime(2021, 1, 1)

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
    service = "Data Pipeline - Forecast Assignments (Active and Inactive)"
    config = load_config(project_id, service)
    client = forecast_client(project_id)

    start_date = datetime(2021, 4, 1)

    assignments_list = []
    while start_date < datetime.today() + timedelta(days=800):
        end_date = start_date + timedelta(days=179)
        assignments_active = unwrap_forecast_response(
            client.get_assignments(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )
        )
        assignments_inactive = unwrap_forecast_response(
            client.get_assignments(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                state="inactive",
            )
        )
        start_date += timedelta(days=180)
        assignments_list += assignments_active + assignments_inactive

    assignments_df = pd.DataFrame(assignments_list).drop_duplicates()
    if len(assignments_list) > 0:
        forecast_assignment_data = expand_assignments_rows(assignments_df)

        forecast_assignment_data = forecast_assignment_data[
            pd.to_datetime(forecast_assignment_data["end_date"]) > START_DATE
        ]

        forecast_assignment_data["hours"] = (
            forecast_assignment_data["allocation"] / 3600
        )
        forecast_assignment_data["days"] = forecast_assignment_data["hours"] / 8

    write_to_bigquery(config, forecast_assignment_data, "WRITE_TRUNCATE")
    print("Done")


def expand_assignments_rows(df):
    # When an assignment is entered, it can be put in for a single day or multiple.
    # For entries spanning across multiple days, this function converts to single day entries and returns the dataframe.
    if "placeholder_id" in df.columns:
        df = df.drop(columns=["placeholder_id"])
    rows_to_edit = df[df["start_date"] != df["end_date"]]
    single_assignment_rows = df[df["start_date"] == df["end_date"]]
    edited_rows = []

    for _, row in rows_to_edit.iterrows():
        # get the times
        end_date = datetime.strptime(row["end_date"], "%Y-%m-%d")
        start_date = datetime.strptime(row["start_date"], "%Y-%m-%d")

        for date in get_dates(start_date, end_date):
            edited_rows.append(make_assignments_row(copy.copy(row), date))

    return pd.concat([single_assignment_rows, pd.DataFrame(edited_rows)])


def get_dates(start_date: datetime, end_date: datetime) -> list:
    date = copy.copy(start_date)
    dates_list = []
    while date <= end_date:
        if date.weekday() < 5:
            dates_list.append(date)
        date = date + timedelta(days=1)
    return dates_list


def make_assignments_row(row: pd.Series, date: datetime) -> pd.Series:
    string_date = datetime.strftime(date, "%Y-%m-%d")
    row["start_date"] = string_date
    row["end_date"] = string_date
    return row


if __name__ == "__main__":
    main({})
