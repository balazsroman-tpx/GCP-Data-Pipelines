import copy
import json
import os
from datetime import datetime, timedelta

import httpx
import pandas as pd

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


def main(data: dict, context: dict = None):
    service = "Data Pipeline - HiBob Time Off"
    config = load_config(project_id, service)
    df = get_holidays(config, httpx.Client())
    write_to_bigquery(config, df, "WRITE_TRUNCATE")


def get_holidays(config: dict, client: httpx.Client):
    start_timestamp = (datetime.now() - timedelta(days=10000)).strftime("%Y-%m-%d")
    end_timestamp = (datetime.now() + timedelta(days=10000)).strftime("%Y-%m-%d")

    url = f"https://api.hibob.com/v1/timeoff/whosout?from={start_timestamp}&to={end_timestamp}&includeHourly=false&includePrivate=true"

    resp = json.loads(client.get(url, headers=config["headers"], timeout=None).text)

    df = expand_holidays_rows(pd.DataFrame(resp["outs"]))

    df["holiday_hours"] = df.apply(lambda row: find_hours(row), axis=1)
    df["holiday_days"] = df["holiday_hours"] / 8
    df["allocation_hours"] = 0
    df["allocation_days"] = 0
    return change_holidays_columns(df)


def find_hours(row: pd.Series) -> int:
    if row["startPortion"] == "all_day" and row["endPortion"] == "all_day":
        return 8
    return 4


def expand_holidays_rows(df: pd.DataFrame) -> pd.DataFrame:
    # When an assignment is entered, it can be put in for a single day or multiple.
    # For entries spanning across multiple days, this function converts to single day entries and returns the dataframe.
    edited_rows = []
    for _, row in df[df["startDate"] != df["endDate"]].iterrows():
        # get the times
        end_date = datetime.strptime(row["endDate"], "%Y-%m-%d")
        start_date = datetime.strptime(row["startDate"], "%Y-%m-%d")

        dates = get_dates(start_date, end_date)

        first_row = copy.copy(row)
        if len(dates) > 1:
            first_row["endPortion"] = "all_day"
            middle_rows = copy.copy(row)
            middle_rows["startPortion"] = "all_day"
            middle_rows["endPortion"] = "all_day"
            for date in dates[1:-1]:
                edited_rows.append(make_holiday_row(copy.copy(middle_rows), date))
            last_row = copy.copy(row)
            last_row["startPortion"] = "all_day"
            edited_rows.append(make_holiday_row(copy.copy(last_row), dates[-1]))
        edited_rows.append(make_holiday_row(first_row, dates[0]))

    return pd.concat([df[df["startDate"] == df["endDate"]], pd.DataFrame(edited_rows)])


def make_holiday_row(row: pd.Series, date: datetime) -> pd.Series:
    string_date = datetime.strftime(date, "%Y-%m-%d")
    row["startDate"] = string_date
    row["endDate"] = string_date
    return row


def change_holidays_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["billable"] = False
    df["entry_type"] = "holiday"
    df["project_id"] = 509809

    return df.drop(
        [
            "startPortion",
            "endPortion",
        ],
        axis=1,
    ).rename(
        columns={
            "endDate": "end_date",
            "startDate": "start_date",
            "requestId": "id",
            "employeeDisplayName": "name",
        }
    )


def get_dates(start_date: datetime, end_date: datetime) -> list[datetime]:
    date = copy.copy(start_date)
    dates_list = []
    while date <= end_date:
        if date.weekday() < 5:
            dates_list.append(date)
        date = date + timedelta(days=1)
    return dates_list


if __name__ == "__main__":
    main({})
