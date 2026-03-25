from http.client import HTTPException
from typing import Any
from google.cloud import storage
from google.cloud.exceptions import Conflict
from decorators import logger, app_logger
from env_vars import project_id
import crc32c

# Don't forget to commment code
#storage_client: storage.Client = storage.Client(project=project_id)

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

@app_logger
def crc_hash_exists(bucket: storage.Bucket, filepath: str):
    with open(filepath, 'rb') as file:
        crc_to_check = crc32c.crc32c(file.read())
        for blob in bucket.list_blobs():
            blob: storage.Blob
            if(blob.crc32c == crc_to_check):
                return True
    return False

# We can change our design approach for this method later
@app_logger
def add_to_storage(input_data_path: str, main_folder: str, partitions: dict[str, Any]):
    """
    Args:
        input_data_path: the path on your system to the data you want t upload
        partitions: the file/folder structure. Please make sure to have the elements in the order you want 
        them to appear in the 
        ie {
        'year': 2025,
        'month': 3
        }
    """
    # get/initialize the bucket
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

    # check checksum
    if not crc_hash_exists(bucket, input_data_path):
        
        # construct the name/folder hierarchy of the blob
        blob_name = main_folder + '/'
        file_name = input("Enter desired blob file name: ")
        for elem in partitions.items():
            blob_name += f"{elem[0]}={elem[1]}/"
        blob_name += file_name

        # get/initialize the blob
        blob = check_blob_existence(bucket, blob_name)
        if(blob is None):
            # probably need a try catch here
            blob = bucket.blob(blob_name) 
        
        # Try to upload data to blob
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
