from google.cloud import bigquery
from services.decorators import app_logger, log_to_app
from services.env_vars import project_id
from google.api_core.exceptions import GoogleAPIError, BadRequest, NotFound, Forbidden
import os
from re import match

query_client: bigquery.Client = bigquery.Client(project=project_id)

# used code from this tutorial: https://docs.cloud.google.com/bigquery/docs/datasets#python
@log_to_app
def create_dataset(dataset_id: str):
    """ Creates a bigquery database with a provided id
        Args:
            dataset_id: the id for the dataset that is in the form of [project_id].[dataset_id]
    """
    try:
        ds = bigquery.Dataset(dataset_id)
        ds.location = 'US'
        ds = query_client.create_dataset(dataset=ds)
    except ValueError:
        app_logger.error("If you have not specified a default project, the dataset_id must also indicate it <project_id>.<dataset_name>")
        raise
    except BadRequest:
        if(dataset_id):
            app_logger.error(f"Dataset_id ({dataset_id}) does not conform to requirements of alphanumeric with the expection of uderscores.")
        raise
    except Exception as e:
        app_logger.exception(f"An exception that I was not aware existed. {e}")

@log_to_app
def create_table(dataset_name: str, table_name: str):
    """ Creates the bigquery external table for all the sales
        Args:
            table_name: name of table"""
    try:
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        
        # Define the External Link (No data is moved)
        ext_config = bigquery.ExternalConfig("PARQUET")
        ext_config.source_uris = [f"gs://tai-project2-bucket/sales_data/*"]
        
        hive_options = bigquery.HivePartitioningOptions()
        hive_options.source_uri_prefix = f"gs://tai-project2-bucket/sales_data/"
        hive_options.mode = "AUTO"
        ext_config.hive_partitioning = hive_options

        table = bigquery.Table(table_id)
        table.external_data_configuration = ext_config

        # This creates the table definition in BQ
        query_client.create_table(table, exists_ok=True)
        app_logger.info(f"External table {table_id} created/verified.")

    except NotFound as e:
        app_logger.error("Error: The GCS file or BQ dataset was not found.")
    except Forbidden as e:
        app_logger.error("Error: Permission denied. Check IAM roles.")
    except BadRequest as e:
        app_logger.error("Error: Schema mismatch or invalid configuration.")
    except Exception as e:
        app_logger.exception(f"An unexpected error occurred: {e}")

@log_to_app
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

def verify_table_exists(dataset_id: str, table_id: str):
    full_table_path = f"{project_id}.{dataset_id}.{table_id}"
    try:
        table = query_client.get_table(full_table_path)
        print(f"✅ Success: Table {table.table_id} exists.")
        print(f"Table Type: {table.table_type}") # Should be 'EXTERNAL'
        print(f"External Source URIs: {table.external_data_configuration.source_uris}") # type: ignore
    except NotFound:
        print(f"❌ Error: Table {full_table_path} was not found.")

# create_dataset(f'{project_id}.tai_cloud_project_dataset')
# check_dataset_existence(f'{project_id}.tai_cloud_project_dataset')
# create_table('tai_cloud_project_dataset', 'transactions')
# verify_table_exists('tai_cloud_project_dataset', 'transactions')