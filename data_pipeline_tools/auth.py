from google.cloud import secretmanager
import forecast


def harvest_headers(project_id, service):
    return {
        "User-Agent": "TPX Cloud Functions",
        "Authorization": "Bearer "
        + access_secret_version(project_id, "HARVEST_ACCESS_TOKEN"),
        "Harvest-Account-ID": access_secret_version(project_id, "HARVEST_ACCOUNT_ID"),
        "service": service,
        "Content-Type": "application/json",
    }


def hibob_headers(project_id, service):
    return {
        "accept": "application/json",
        "Authorization": access_secret_version(project_id, "BOB_ACCESS_TOKEN"),
        "service": service,
    }


def pipedrive_access_token(project_id):
    return access_secret_version(project_id, "PIPEDRIVE_ACCESS_TOKEN")


def service_account_json(project_id):
    return access_secret_version(project_id, "SERVICE_ACCOUNT_JSON")


def forecast_client(project_id):
    return forecast.Api(
        account_id=forecast_account_id(project_id),
        auth_token=forecast_access_token(project_id),
    )


def forecast_account_id(project_id):
    return access_secret_version(project_id, "FORECAST_ACCOUNT_ID")


def forecast_access_token(project_id):
    return access_secret_version(project_id, "FORECAST_ACCESS_TOKEN")


def access_secret_version(
    project_id: str, secret_id: str, version_id: str = "latest"
) -> str:
    # Create the Secret Manager client and get the secret payload.
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")
