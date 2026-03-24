from google.cloud import bigquery
from decorators import logger, app_logger
from env_vars import project_id
from google.api_core.exceptions import GoogleAPIError, BadRequest
import os
from re import match

query_client: bigquery.Client = bigquery.Client(project=project_id)

@app_logger
def create_dataset(dataset_id: str):
    """ Creates a bigquery database with a provided id
        Args:
            dataset_id: the id for the dataset that is in the form of [project_id].[dataset_id]
        
        Used code form this tutorial: https://docs.cloud.google.com/bigquery/docs/datasets#python"""
    try:
        ds = bigquery.Dataset(dataset_id)
        ds.location = 'US'
        ds = query_client.create_dataset(dataset=ds)
    except ValueError:
        logger.error("If you have not specified a default project, the dataset_id must also indicate it <project_id>.<dataset_name>")
        raise
    except BadRequest:
        if(dataset_id):
            logger.error(f"Dataset_id ({dataset_id}) does not conform to requirements of alphanumeric with the expection of uderscores.")
        raise
    except Exception:
        logger.exception("An exception that I was not aware existed.")

@app_logger
def create_tables():
    pass

@app_logger
def check_dataset_existence(ds_id: str) -> bigquery.Dataset | None:
    """ Checks if a dataset is in the project
        Args:
            ds_id: id or name of the dataset to find
        Returns:
            bigquery.Dataset with the specified id: if the id exists
            None: if the dataset doesn't exist
    """
    for ds in query_client.list_datasets(project=project_id):
        ds: bigquery.Dataset
        if(ds.dataset_id == ds_id):
            return ds
    return None


create_dataset(f'{project_id}.tai_cloud_project_dataset')