import json

import pandas as pd
import pandas_gbq
import requests
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.oauth2 import service_account

from .auth import service_account_json


def read_from_bigquery(project_id: str, query: str) -> pd.DataFrame:
    """
    Reads data from a BigQuery table and returns it as a Pandas DataFrame.

    Args:
        project_id (str): The ID of the Google Cloud project that contains the BigQuery table.
        query (str): The SQL query to execute on the BigQuery table.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the data from the BigQuery table.
    """
    # Load the credentials from the service account JSON file
    credentials_info = json.loads(service_account_json(project_id))
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info, scopes=["https://www.googleapis.com/auth/bigquery"]
    )

    # Read the data from the BigQuery table and return it as a DataFrame
    return pandas_gbq.read_gbq(query, project_id=project_id, credentials=credentials)


def write_to_bigquery(config: dict, df: pd.DataFrame, write_disposition: str) -> None:
    # config must contain the following keys:
    # - dataset_id
    # - table_name
    # - location
    # Create a BigQuery client with the specified location.
    client = bigquery.Client(location=config["location"])

    # Get a reference to the BigQuery table to write to.
    dataset_ref = client.dataset(config["dataset_id"])
    table_ref = dataset_ref.table(config["table_name"])

    # Set up the job configuration with the specified write disposition.
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job_config.autodetect = True

    try:
        # Write the DataFrame to BigQuery using the specified configuration.
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
    except BadRequest as e:
        print(f"Error writing DataFrame to BigQuery: {str(e)}")
        return

    # Print a message indicating how many rows were loaded.
    print(
        "Loaded {} rows into {}:{}.".format(
            job.output_rows, config["dataset_id"], config["table_name"]
        )
    )


def find_and_flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Find all columns that have a dictionary as a value in the first row of the DataFrame.
    nested_columns = [
        column_name
        for column_name, value in df.iloc[0].items()
        if isinstance(value, dict)
    ]
    return flatten_columns(df, nested_columns)


def flatten_columns(df: pd.DataFrame, nested_columns: list) -> pd.DataFrame:
    # For each nested column, flatten the JSON values using Pandas' json_normalize function.
    for column in nested_columns:
        # Convert the column values to dictionaries if they are integers.
        df[column] = df[column].apply(
            lambda x: x if not isinstance(x, int) else {"value": x}
        )
        # Convert the column values to dictionaries if they are None.
        df[column] = df[column].apply(lambda x: x if not None else {})

        print(f"Flattening column: {column}")
        flattened_df = pd.json_normalize(df[column], max_level=1).add_prefix(
            f"{column}_"
        )

        # Add the flattened columns to the DataFrame and drop the original nested column.
        df = pd.concat([df, flattened_df], axis=1)
        df = df.drop(column, axis=1)

    return df


def get_harvest_pages(url: str, headers: dict):
    url = f"{url}1"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
        data = response.json()

        return data["total_pages"], data["total_entries"]
    except (requests.exceptions.RequestException, KeyError) as e:
        print(f"Error retrieving total pages: {e}")
        return None
