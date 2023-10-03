import os
from data_pipeline_tools.util import write_to_bigquery, read_from_bigquery
from datetime import datetime
import random
import pandas as pd
import calendar


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
    service = "Data Pipeline - Harvest Timesheets Filled"
    config = load_config(project_id, service)
    days = get_dates_list()
    print(days[0], "-", days[-1])

    QUERY = f"""
    SELECT * FROM `{config['gcp_project']}.Harvest_Raw.timesheets`
    WHERE DATE(spent_date) > "2022-03-31"
    AND DATE(spent_date) < "2024-03-31"
    """
    df = read_from_bigquery(project_id, QUERY)
    Q2 = f"""SELECT id FROM `{config['gcp_project']}.Harvest_Raw.users`
    WHERE is_active = false"""
    users_df = read_from_bigquery(project_id, Q2)

    for person_id in users_df["id"].to_list():
        temp_df = df[df["person_id"] == person_id]
        already_filled_dates = temp_df["spent_date"].to_list()
        blank_dates = [x for x in days if x not in already_filled_dates]
        entries = list(
            map(
            lambda day:
                [{
                    "id": random.randint(100000000, 999999999),
                    "spent_date": day,
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
                },{'id': 1770259615, 'spent_date':day, 'hours': 1.0, 'hours_without_timer': 1.0, 'rounded_hours': 1.0, 'notes': None, 'is_locked': True, 'locked_reason': 'Item Archived', 'is_closed': False, 'is_billed': False, 'timer_started_at': None, 'started_time': None, 'ended_time': None, 'is_running': False, 'billable': False, 'budgeted': False, 'billable_rate': nan, 'cost_rate': 34.55, 'created_at': '2022-04-27T11:56:40Z', 'updated_at': '2022-12-09T10:51:52Z', 'invoice': None, 'external_reference': None, 'user_id': 2989713, 'user_name': 'Joseph Curle', 'client_id': 11945933, 'client_name': 'TPXimpact', 'client_currency': 'GBP', 'project_id': 8949317, 'project_name': 'Sales - Proposals, bids', 'project_code': 'Business Development', 'task_id': 3707315, 'task_name': 'Billable', 'user_assignment_id': 346479860, 'user_assignment_is_project_manager': False, 'user_assignment_is_active': False, 'user_assignment_use_default_rates': False, 'user_assignment_budget': nan, 'user_assignment_created_at': '2022-05-12T13:32:42Z', 'user_assignment_updated_at': '2022-12-09T10:51:52Z', 'user_assignment_hourly_rate': 0.0, 'task_assignment_id': 97552265, 'task_assignment_billable': False, 'task_assignment_is_active': False, 'task_assignment_created_at': '2015-09-23T10:55:57Z', 'task_assignment_updated_at': '2016-03-01T18:15:14Z', 'task_assignment_hourly_rate': 0.0, 'task_assignment_budget': nan, 'utilisation': 0.0}], blank_dates
            )
        )
        

    final_df = pd.concat([df, pd.DataFrame(entries)])
    columns_to_drop = []
    final_df = final_df.drop(columns=columns_to_drop, errors="ignore")
    
    write_to_bigquery(config, final_df, "WRITE_TRUNCATE")


def get_dates_list():
    # Define the financial year start month and day
    financial_year_start_month = 4

    # Get the current date
    current_date = datetime.now().date()
    current_year = current_date.year
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
