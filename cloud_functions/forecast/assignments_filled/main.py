import os
from data_pipeline_tools.util import write_to_bigquery, read_from_bigquery
from data_pipeline_tools.auth import pipedrive_access_token, service_account_json
from datetime import datetime
import random
import pandas as pd
import calendar


import_copy = os.environ.get("IMPORT_COPY")

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
    service = "Data Pipeline - Forecast Assignments Filled"
    config = load_config(project_id, service)
    days = get_dates_list()

    QUERY = f"""
    SELECT * FROM `tpx-cheetah.Forecast_Raw.assignments`
    WHERE DATE(start_date) > "2022-03-31"
    AND DATE(start_date) < "2024-03-31"
    """
    df = read_from_bigquery(project_id, QUERY)
    Q2 = f"""SELECT id FROM `tpx-cheetah.Forecast_Raw.people`
    WHERE archived = false"""
    people_df = read_from_bigquery(project_id, Q2)

    entries = []
    for person_id in people_df["id"].to_list():
        temp_df = df[df["person_id"] == person_id]
        already_filled_dates = temp_df["start_date"].to_list()
        blank_dates = [x for x in days if x not in already_filled_dates]

        for day in blank_dates:
            entries.append(
                {
                    "id": random.randint(100000000, 999999999),
                    "start_date": day,
                    "end_date": day,
                    "allocation": 14400.0,
                    "notes": None,
                    "updated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "updated_by_id": 9999999,
                    "project_id": 999999,
                    "person_id": person_id,
                    "repeated_assignment_set_id": None,
                    "active_on_days_off": False,
                    "hours": 8.0,
                    "days": 1,
                }
            )

    final_df = pd.concat([df, pd.DataFrame(entries)])
    write_to_bigquery(config, final_df, "WRITE_TRUNCATE")


def get_dates_list():
    # Define the financial year start month and day
    financial_year_start_month = 4

    # Get the current date
    current_date = datetime.now().date()
    if current_date.month < financial_year_start_month:
        current_year = current_date.year - 1
    months = (
        [datetime(current_year, x, 1) for x in range(4, 13)]
        + [datetime(current_year + 1, x, 1) for x in range(1, 13)]
        + [datetime(current_year + 2, x, 1) for x in range(1, 4)]
    )
    days = []
    for month in months:
        days += get_weekdays(month.year, month.month)
    return days


def get_weekdays(year, month):
    # Get the number of days in the month and the weekday of the first day of the month
    first_day = 1
    num_days = calendar.monthrange(year, month)[1]
    first_weekday = calendar.weekday(year, month, first_day)

    weekdays = []
    # Determine the day number of the first weekday in the month

    if first_weekday < 5:  # Monday = 0, Friday = 4
        for x in range(5 - first_weekday):
            weekdays.append(datetime(year, month, x + 1).strftime("%Y-%m-%d"))
            first_day += 1
    if first_weekday == 6:
        first_day += 1
    else:
        first_day += 2

    # Generate a list of all the weekdays in the month

    for days in range(first_day, num_days + 1, 7):

        weekdays.extend(
            [
                datetime(year, month, day).strftime("%Y-%m-%d")
                for day in range(days, min(days + 5, num_days + 1))
            ]
        )

    return weekdays


if __name__ == "__main__":
    main({})
