import sys
import pandas as pd
import os

sys.path.insert(0, "../../..")
from data_pipeline_tools.forecast_tools import forecast_client
from data_pipeline_tools.util import unwrap_forecast_response, write_to_bigquery


project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
if not project_id:
    project_id = input("Enter GCP project ID: ")


def load_config(project_id, service) -> dict:
    return {
        "dataset_id": os.environ.get("DATASET_ID"),
        "gcp_project": project_id,
        "table_name": os.environ.get("TABLE_NAME"),
        "location": os.environ.get("TABLE_LOCATION"),
        "service": service,
    }


def main(data: dict, context: dict = None):
    service = "Data Pipeline - Forecast People"
    config = load_config(project_id, service)
    client = forecast_client(project_id)
    people_resp = unwrap_forecast_response(client.get_people())

    people_df = pd.DataFrame(people_resp)

    people_df["working_days"] = people_df["working_days"].apply(
        lambda working_days: list(working_days.values()).count(True)
    )
    people_df["weekly_capacity"] = people_df["weekly_capacity"] / (3600 * 8)

    people_df["external"] = people_df["roles"].apply(lambda row: "associate" in row)

    # people_df["role"] = people_df["roles"].apply(lambda row: get_role(row))
    write_to_bigquery(config, people_df, "WRITE_TRUNCATE")
    print("Done")


our_roles = [
    "CST",
    "Delivery Manager",
    "Engagement Team",
    "Service Design",
    "Design Research",
    "Interaction Design",
    "Content Design",
    "Organisational Design Consultant",
    "Engineering",
    "Questers",
    "Academy",
]


def get_role(roles):

    these_roles = []
    for role in roles:
        for our_role in our_roles:
            if role.startswith(our_role):
                these_roles.append(role)
    if len(these_roles) > 1:
        if "Engineering" in these_roles:
            these_roles.remove("Engineering")
        if "Design Research" in these_roles:
            these_roles.remove("Design Research")
    if len(these_roles) > 1:
        raise Exception("more than one role")
    if len(these_roles) == 1:
        return these_roles[0]
    return None


if __name__ == "__main__":
    main({})
