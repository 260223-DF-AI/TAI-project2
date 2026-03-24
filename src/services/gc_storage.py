from http.client import HTTPException
from google.cloud import storage
from google.cloud.exceptions import Conflict
from decorators import logger, app_logger
from env_vars import project_id

# Don't forget to commment code
storage_client: storage.Client = storage.Client(project=project_id)

@app_logger
def initialize_sclient():
    try:
        global storage_client
        storage_client = storage.Client(project=project_id)
    except Exception as e:
        logger.exception(e)
        raise

@app_logger
def check_bucket_existence(name: str) -> storage.Bucket | None:
    """ Tries to find the bucket with the specified name from the project
    Args: 
        name: name of the bucket to check
    Returns:
        bucket: if it exists
        None: otherwise
    """
    for bucket in storage_client.list_buckets(project=project_id):
        if(bucket.name == name):
            return bucket
    return None

@app_logger
def check_blob_existence(bucket: storage.Bucket, blob_name: str) -> storage.Blob | None:
    """ Tries to find if a blob exists in a given/specified bucket
    Args:
        bucket: the bucket the blob is in
        blob_name: name of the blob to check
    Returns:
        blob: if it exists
        None: otherwise
    """
    for blob in storage_client.list_blobs(bucket_or_name=bucket):
        if(blob.name == blob_name):
            return blob
    return None

# We can change our design approach for this method later
@app_logger
def add_to_storage(input_data_path):

    # get/set the bucket
    bucket_name = input("Enter the name of your desired bucket: ")
    bucket = check_bucket_existence(bucket_name)
    if(bucket is None):
        try:
            bucket = storage_client.create_bucket(bucket_or_name=bucket_name, project=project_id)
        except Conflict:
            # log and raise exception
            logger.error("Bucket name needs to be unique across all gcstorage users and buckets.")
            raise
        except Exception as e:
            # log and raise exception
            logger.exception(e)
            raise

    # get/set the blob from bucket
    blob_name = input("Enter the name of the blob to store your data: ")
    blob = check_blob_existence(bucket, blob_name)
    if(blob is None):
        # probably need a try catch here
        blob = bucket.blob(blob_name) 
    
    # need to make this, especially the chunk_size more dynamic in the future
    # need checksum eventually
    # Stream buffer to cap RAM usage it is allowed
    try:
        blob.upload_from_filename(input_data_path, checksum='crc32c')
    except HTTPException:
        logger.error("Change message later")
        raise
    except Exception as e:
        logger.exception(e)
        raise

@app_logger
def delete_blob(bucket: storage.Bucket, blob_name: str):
    blob = check_blob_existence(bucket, blob_name)
    if(blob is not None):
        blob.delete()

@app_logger
def delete_bucket(bucket_name: str):
    bucket = check_bucket_existence(bucket_name)
    if(bucket is not None):
        bucket.delete(force=True)
