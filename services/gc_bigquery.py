from google.cloud import bigquery
from services.decorators import logger, app_logger
from services.env_vars import project_id
from google.api_core.exceptions import GoogleAPIError, BadRequest, NotFound, Forbidden
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
    except Exception as e:
        logger.exception(f"An exception that I was not aware existed. {e}")

@app_logger
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
        hive_options.mode = "CUSTOM"
        hive_options.fields = [
            bigquery.SchemaField("year", "INT64"),
            bigquery.SchemaField("month", "INT64")
        ]

        ext_config.hive_partitioning_options = hive_options

        table = bigquery.Table(table_id)
        table.external_data_configuration = ext_config

        # This creates the table definition in BQ
        query_client.create_table(table, exists_ok=True)
        logger.info(f"External table {table_id} created/verified.")

    except NotFound as e:
        logger.error("Error: The GCS file or BQ dataset was not found.")
    except Forbidden as e:
        logger.error("Error: Permission denied. Check IAM roles.")
    except BadRequest as e:
        logger.error("Error: Schema mismatch or invalid configuration.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")

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

def verify_table_exists(dataset_id: str, table_id: str):
    full_table_path = f"{project_id}.{dataset_id}.{table_id}"
    try:
        table = query_client.get_table(full_table_path)
        print(f"✅ Success: Table {table.table_id} exists.")
        print(f"Table Type: {table.table_type}") # Should be 'EXTERNAL'
        print(f"External Source URIs: {table.external_data_configuration.source_uris}")
    except NotFound:
        print(f"❌ Error: Table {full_table_path} was not found.")

def get_sales_total_by_store(store_id: str):
    query = """
            SELECT SUM(TotalAmount) as total_amount
            FROM `tai_cloud_project_dataset.transactions`
            WHERE StoreID = @str_id
        """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("str_id", "STRING", store_id),
        ]
    )
    df = query_client.query(query, job_config=job_config).to_dataframe()
    return df

def sales_total():
    query = """
            SELECT SUM(TotalAmount) as total_amount
            FROM `tai_cloud_project_dataset.transactions`
        """ 
    
    df = query_client.query(query).to_dataframe()
    return df

def get_sales_total_by_product(product_id: str):
    query = """
            SELECT COALESCE(SUM(TotalAmount), 0) AS total_amount
            FROM `tai_cloud_project_dataset.transactions`
            WHERE ProductID = @pr_id
        """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pr_id", "STRING", product_id),
        ]
    )

    df = query_client.query(query, job_config=job_config).to_dataframe()
    return df

def get_highest_unit_product():
    query = """
            SELECT ProductName, COALESCE(SUM(Quantity), 0) AS quantity
            FROM `tai_cloud_project_dataset.transactions`
            GROUP BY ProductName ORDER BY quantity DESC
            LIMIT 1
        """

    df = query_client.query(query).to_dataframe()
    return df

def get_highest_unit_product_month(month: int):
    query = """
            SELECT ProductName, COALESCE(SUM(Quantity), 0) AS quantity
            FROM `tai_cloud_project_dataset.transactions`
            WHERE month = @m GROUP BY ProductName 
            ORDER BY quantity DESC
            LIMIT 1
        """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("m", "INT64", month),
        ]
    )

    df = query_client.query(query, job_config=job_config).to_dataframe()
    return df

def get_customer_summary(customer_id: str):
    query = """
            SELECT CustomerName, COUNT(CustomerID) as transaction_count, COALESCE(SUM(TotalAmount), 0) AS total_amount
            FROM `tai_cloud_project_dataset.transactions`
            WHERE CustomerID = @cs_id GROUP BY CustomerName
        """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cs_id", "STRING", customer_id),
        ]
    )

    df = query_client.query(query, job_config=job_config).to_dataframe()
    return df