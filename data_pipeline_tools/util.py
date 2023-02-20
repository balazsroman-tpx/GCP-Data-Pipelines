import pandas as pd
from google.cloud import bigquery, secretmanager


def write_to_bigquery(config: dict, df: pd.DataFrame, write_disposition: str):
    client = bigquery.Client(location=config["location"])

    dataset_ref = client.dataset(config["dataset_id"])
    table_ref = dataset_ref.table(config["table_name"])
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job_config.autodetect = True

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(
        "Loaded {} rows into {}:{}.".format(
            job.output_rows, config["dataset_id"], config["table_name"]
        )
    )


def flatten_columns(df):
    # Flatten nested JSON values using Pandas' json_normalize function
    nested_columns = [
        column_name
        for column_name, value in df.iloc[0].items()
        if isinstance(value, dict)
    ]

    for column in nested_columns:
        flattened_df = pd.json_normalize(df[column], max_level=1).add_prefix(
            f"{column}_"
        )

        flattened_df = flattened_df

        df = pd.concat([df, flattened_df], axis=1)
        df = df.drop(column, axis=1)

    return df


def access_secret_version(project_id, secret_id, version_id="latest"):
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(name=name)

    # Return the decoded payload.
    return response.payload.data.decode("UTF-8")
