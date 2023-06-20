from datetime import datetime, timedelta
import pandas as pd
import holidays

def get_uk_holidays(year=datetime.now().year):
    print("Getting UK holidays")
    holidays_resp = list(
        map(
            lambda date: {"date": date[0], "name": date[1]},
            holidays.UK(years=year).items(),
        )
    )
    holidays_df = pd.DataFrame(holidays_resp).sort_values("date").reset_index(drop=True)
    holidays_df["spent_date"] = holidays_df["date"].apply(
        lambda date: get_spent_dates(date)
    )
    return holidays_df[
        ~holidays_df["name"].str.contains(r"\[Scotland\]")
        & ~holidays_df["name"].str.contains(r"\[Northern Ireland\]")
    ]

def get_spent_dates(date):
    if date.weekday() == 5:
        return date + timedelta(days=2)
    elif date.weekday() == 6:
        return date + timedelta(days=1)
    return date