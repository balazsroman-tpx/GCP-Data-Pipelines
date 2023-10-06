import forecast

from data_pipeline_tools.auth import access_secret_version


def forecast_client(project_id):
    return forecast.Api(
        account_id=forecast_account_id(project_id),
        auth_token=forecast_access_token(project_id),
    )


def forecast_account_id(project_id):
    return access_secret_version(project_id, "FORECAST_ACCOUNT_ID")


def forecast_access_token(project_id):
    return access_secret_version(project_id, "FORECAST_ACCESS_TOKEN")
