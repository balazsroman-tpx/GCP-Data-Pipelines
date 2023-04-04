# GCP-Data-Pipelines

This repository contains a suite of Python-based cloud functions that collect, process, and integrate data from various APIs for the HR and finance systems. The cloud functions are deployed to Google Cloud using Terraform, and package management is handled with Poetry. The purpose of these functions is to gather essential details and metrics related to booked hours, forecasted hours, and deals in Pipedrive for the company's HR system. The data collected is stored in Google BigQuery, where SQL queries will be used to generate tables that can be connected to Google Data Studio for insightful reporting.

## Features

- Collection of 10-20 Python cloud functions
- Integration with HiBob, Harvest, Forecast, and Pipedrive APIs
- Pricing package containing reusable functions used across the cloud functions
- Deployment to Google Cloud with Terraform
- Package management using Poetry (with requirements.txt export)
- Data storage in Google BigQuery with raw tables
- Planned integration with Google Data Studio for report generation using SQL queries
- CI/CD with cloud build will be coming

## Cloud Functions List

The following is a list of cloud functions with "pipe" in their name:

1. forecast_assignments_filled_pipe
2. forecast_assignments_pipe
3. forecast_people_pipe
4. forecast_projects_pipe
5. harvest_clients_pipe
6. harvest_projects_pipe
7. harvest_timesheet_pipe
8. harvest_user_project_assignments_pipe
9. harvest_users_pipe
10. hibob_time_off_pipe
11. pipedrive_deals_pipe
12. pipedrive_organisations_pipe

These cloud functions are part of the HR Data Integration and Reporting system and are responsible for processing data from various APIs.

## Getting Started

### Prerequisites

- Python 3.10 or later
- Terraform
- Poetry
- Access to GCP

### Installation

1. Clone this repository:

    `git clone https://github.com/tpximpact/GCP-Data-Pipelines.git`


2. Navigate to the project directory:

    `cd cloud_functions/api_name/endpoint_name`

3. Install dependencies using Poetry:

    `poetry install`


4. Export the dependencies to a `requirements.txt` file
    (This needs to be done to deploy to Google cloud):
    
    `poetry export --format=requirements.txt --output=requirements.txt --without-hashes`


5. Set up Terraform:
    
    `terraform init`


6. Apply the Terraform configuration:
    
    `terraform apply`



### Usage

The cloud functions in this repository connect to HiBob, Harvest, Forecast, and Pipedrive APIs to collect and process data for the HR and Finance systems. The collected data is stored in raw tables within Google BigQuery. In the future, SQL queries will be added to generate tables that can be connected to Google Data Studio for creating comprehensive reports on the gathered data.

