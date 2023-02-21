from .util import access_secret_version


def harvest_headers(project_id, service):
    return {
        "User-Agent": "TPX Cloud Functions",
        "Authorization": "Bearer "
        + access_secret_version(project_id, "HARVEST_ACCESS_TOKEN"),
        "Harvest-Account-ID": access_secret_version(project_id, "HARVEST_ACCOUNT_ID"),
        "service": service,
    }

def pipedrive_access_token(project_id):
    return access_secret_version(project_id, "PIPEDRIVE_ACCESS_TOKEN")
    