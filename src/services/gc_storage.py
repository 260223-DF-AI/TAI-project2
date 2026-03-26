import base64
from http.client import HTTPException
from typing import Any
from google.cloud import storage
from google.cloud.exceptions import Conflict
from services.decorators import app_logger, audit_logger, log_to_app, log_to_audit
from services.env_vars import project_id
import crc32c
from pathlib import Path
import os

# Don't forget to commment code
storage_client: storage.Client = storage.Client(project=project_id)

@log_to_app
def initialize_sclient():
    try:
        global storage_client
        storage_client = storage.Client(project=project_id)
    except Exception as e:
        app_logger.exception(e)
        raise

@log_to_app
def check_bucket_existence(name: str) -> storage.Bucket | None:
    """Tries to find the bucket with the specified name from the project
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

@log_to_app
def check_blob_existence(bucket: storage.Bucket, blob_name: str) -> storage.Blob | None:
    """Tries to find if a blob exists in a given/specified bucket
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


"""There is a caveate to this, it will calculate a different hash if any part of the file is different than what got 
uploaded to GCS, even if the only difference if the metadata (which includes timetimes, etc). Basically there are ways
where files built upon the same data can actually be different files, resulting in differing hashes."""
@log_to_app
@log_to_audit
def crc_hash_exists(bucket: storage.Bucket, filepath: str | Path):
    """Checks the hash of the bundle to be uploaded against the hashes of the bundles/blobs in the bucket
    Args:
        bucket: storage bucket to check the contents of
        filepath: the path to the file to be uploaded
    Return:
        bool: True if the hash exists and false otherwise
        hash: The hash of the bundle to be uploaded
    """
    try:
        with open(filepath, 'rb') as file:
            crc_to_check = base64.b64encode(crc32c.crc32c(file.read()).to_bytes(4,'big')).decode('utf-8')
            for blob in bucket.list_blobs():
                blob: storage.Blob # letting vs code know what object blob is
                print(f"blob hash: {blob.crc32c}") # get rid of this before Friday
                if(blob.crc32c == crc_to_check):
                    app_logger.info("Tried to redownload or upload the same batch of data")
                    return True, crc_to_check
            return False, crc_to_check
    except FileNotFoundError:
        app_logger.error(f"File from path {filepath} could not be found.")
        raise
    except OSError:
        app_logger.error("Couldn't open or read from the provided file, make sure the file stores binary data.")
        raise
    except Exception as e:
        app_logger.exception(e)
        raise

# We can change our design approach for this method later
@log_to_app
def add_to_storage(input_data_path: str | Path, main_folder: str, partitions: dict[str, Any]):
    """
    Args:
        input_data_path: the path on your system to the data you want t upload
        partitions: the file/folder structure. Please make sure to have the keys 
        in the hierarchical order you want for your file structure
        ie {
        'year': 2025,
        'month': 3
        }
    """
    # get/initialize the bucket
    bucket_name = "tai-project2-bucket"
    bucket = check_bucket_existence(bucket_name)
    if(bucket is None):
        try:
            bucket = storage_client.create_bucket(bucket_or_name=bucket_name, project=project_id)
        except Conflict:
            app_logger.error("Bucket name needs to be unique across all gcstorage users and buckets.")
            raise
        except Exception as e:
            app_logger.exception(e)
            raise

    # check checksum
    does_hash_exist, hash = crc_hash_exists(bucket, input_data_path)
    print(f"does hash exist: {does_hash_exist}\nhash: {hash}") # get rid of this before Friday
    if not does_hash_exist:
        # construct the name/folder hierarchy of the blob
        blob_name = main_folder + '/'
        file_name = os.path.splitext(os.path.basename(input_data_path))[0] + ".parquet"
        # file_name = input_data_path[input_data_path.rfind('/') + 1: input_data_path.rfind('.')]
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
            audit_logger.info(f"Uploaded bundle with hash: {blob.crc32c}")
        except HTTPException:
            app_logger.error("Change message later")
            audit_logger.error(f"Failed to upload bundle")
            raise
        except Exception as e:
            app_logger.exception(e)
            raise

@log_to_app
def delete_blob(bucket: storage.Bucket, blob_name: str):
    """Deletes a blob by its name from a given bucket"""

    blob = check_blob_existence(bucket, blob_name)
    if(blob is not None):
        blob.delete()

@log_to_app
def delete_bucket(bucket_name: str):
    """Deletes a bucket by its name from the project"""

    bucket = check_bucket_existence(bucket_name)
    if(bucket is not None):
        bucket.delete(force=True)