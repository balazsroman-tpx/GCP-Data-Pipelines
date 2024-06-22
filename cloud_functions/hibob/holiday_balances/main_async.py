import asyncio
import os
from datetime import date, datetime

import httpx
import pandas as pd
import requests

from data_pipeline_tools.auth import hibob_headers
from data_pipeline_tools.util import write_to_bigquery

project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
if not project_id:
    project_id = input("Enter GCP project ID: ")

CONFIG = {
    "table_name": os.environ.get("TABLE_NAME"),
    "dataset_id": os.environ.get("DATASET_ID"),
    "location": os.environ.get("TABLE_LOCATION"),
    "headers": hibob_headers(project_id, "Data Pipeline - HiBob Holiday Balances"),
}
YEAR = date(datetime.now().year, 12, 31)


def get_employee_ids() -> list[str]:
    response = requests.post(
        "https://api.hibob.com/v1/people/search",
        headers=CONFIG["headers"],
        json={
            "fields": ["root.id"],
            "showInactive": False,
            "humanReadable": "REPLACE",
        },
    )
    response.raise_for_status()
    return [employee["id"] for employee in response.json()["employees"]]


async def get_employee_balance(
    employee_id: str,
    client: httpx.AsyncClient,
) -> dict[str:str]:
    url = f"https://api.hibob.com/v1/timeoff/employees/{employee_id}/balance?policyType=TPXimpact%20Holiday&date={YEAR}"
    response = await client.get(url, headers=CONFIG["headers"])
    return response.json()


async def main(data: dict, context: dict = None):
    timeout = httpx.Timeout(20.0, connect=60.0)
    limits = httpx.Limits(max_connections=40, max_keepalive_connections=20)

    async with httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
    ) as client:
        employee_ids = get_employee_ids()
        semaphore = asyncio.Semaphore(10)  # Limit to 10 concurrent requests

        async def fetch_with_semaphore(employee_id):
            async with semaphore:
                return await get_employee_balance(employee_id, client)

        tasks = [fetch_with_semaphore(employee_id) for employee_id in employee_ids]
        balances = await asyncio.gather(*tasks)

    write_to_bigquery(
        CONFIG,
        pd.DataFrame([balance for balance in balances if balance]),
        "WRITE_TRUNCATE",
    )


if __name__ == "__main__":
    asyncio.run(main({}))
