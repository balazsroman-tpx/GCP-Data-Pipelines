import pandas as pd
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery, secretmanager


def write_to_bigquery(config: dict, df: pd.DataFrame, write_disposition: str) -> None:
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


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Find all columns that have a dictionary as a value in the first row of the DataFrame.

    nested_columns = [
        column_name
        for column_name, value in df.iloc[0].items()
        if isinstance(value, dict)
    ]

    # For each nested column, flatten the JSON values using Pandas' json_normalize function.
    for column in nested_columns:
        flattened_df = pd.json_normalize(df[column], max_level=1).add_prefix(
            f"{column}_"
        )

        # Add the flattened columns to the DataFrame and drop the original nested column.
        df = pd.concat([df, flattened_df], axis=1)
        df = df.drop(column, axis=1)

    return df


def access_secret_version(
    project_id: str, secret_id: str, version_id: str = "latest"
) -> str:
    # Create the Secret Manager client and get the secret payload.
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")