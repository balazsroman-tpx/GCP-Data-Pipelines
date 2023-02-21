from .util import access_secret_version


def harvest_headers(project_id, service):
    {
        "User-Agent": "Harvest Data Visualisation",
        "Authorization": "Bearer "
        + access_secret_version(project_id, "HARVEST_ACCESS_TOKEN"),
        "Harvest-Account-ID": access_secret_version(project_id, "HARVEST_ACCOUNT_ID"),
        "service": service,
    }
