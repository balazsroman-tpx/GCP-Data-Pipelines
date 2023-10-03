import os

import pandas as pd
from pipedrive.client import Client

from data_pipeline_tools.auth import pipedrive_access_token
from data_pipeline_tools.util import flatten_columns, write_to_bigquery

project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
if not project_id:
    # project_id = input("Enter GCP project ID: ")
    project_id = "tpx-consulting-dashboards"


def update_keys(dict_list, keys_to_update, new_keys):
    for dictionary in dict_list:
        for old_key, new_key in zip(keys_to_update, new_keys):
            if old_key in dictionary.keys():
                dictionary[new_key.replace(" ", " ").lower()] = dictionary.pop(old_key)
    return dict_list


def get_option_from_key(key: str, options) -> str:
    if type(key) == str:
        if key.isnumeric():
            option = options[options["id"] == int(key)]["label"].values
            if len(option) > 0:
                return option[0]
            if len(key) > 0:
                return f"{key} Not Found ?!?"
    return key


def load_config(project_id, service) -> dict:
    return {
        "auth_token": pipedrive_access_token(project_id),
        "dataset_id": os.environ.get("DATASET_ID"),
        "gcp_project": project_id,
        "table_name": os.environ.get("TABLE_NAME"),
        "location": os.environ.get("TABLE_LOCATION"),
        "service": service,
    }


def main(data: dict, context: dict = None):
    service = "Data Pipeline - Pipedrive Organisations"
    config = load_config(project_id, service)

    client = Client(domain="https://companydomain.pipedrive.com/")

    client.set_api_token(config["auth_token"])
    print("Pipedrive client created")

    done = False
    organisations = []
    start = 0  # 0 is the first deal
    while not done:
        print(f"Getting organisations from start: {start}")
        organisations_resp = client.organizations.get_all_organizations(
            params={"start": start}
        )
        if not organisations_resp["success"]:
            raise Exception("Error retrieving organisations")
        organisations += organisations_resp["data"]
        if not organisations_resp["additional_data"]["pagination"][
            "more_items_in_collection"
        ]:
            done = True
        else:
            start = organisations_resp["additional_data"]["pagination"]["next_start"]

    print("organisations retrieved")

    org_fields_resp = client.organizations.get_organization_fields()
    if not org_fields_resp["success"]:
        raise Exception("unsuccessful")
    org_fields = pd.DataFrame(
        list(
            map(
                lambda c: {
                    "name": c["name"],
                    "key": c["key"],
                    "options": c.get("options"),
                },
                org_fields_resp["data"],
            )
        )
    )

    optioned_columns = org_fields[org_fields["options"].notnull()]
    update_keys(organisations, org_fields["key"], org_fields["name"])
    orgs_df = pd.DataFrame(organisations).rename(
        columns=lambda x: x.replace(
            " ",
            "_",
        ).lower()
    ) 
    for _, item in optioned_columns.iterrows():
        print(item["name"])
        orgs_df[item["name"].replace(" ", "_").lower()] = orgs_df[
            item["name"].replace(" ", "_").lower()
        ].apply(lambda x: get_option_from_key(x, pd.DataFrame(item["options"])))

    write_to_bigquery(config, orgs_df, "WRITE_TRUNCATE")


if __name__ == "__main__":
    main({}, {})
