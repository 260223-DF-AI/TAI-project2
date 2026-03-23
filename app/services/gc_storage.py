from google.cloud import storage
from env_vars import project_id

# Don't forget to commment code
storage_client: storage.Client = storage.Client(project=project_id)

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
def add_to_storage(input_data_path):

    # get/set the bucket
    bucket_name = input("Enter the name of your desired bucket: ")
    bucket = check_bucket_existence(bucket_name)
    if(bucket is None):
        # need a try catch here
        bucket = storage_client.create_bucket(bucket_or_name=bucket_name, project=project_id)

    # get/set the blob from bucket
    blob_name = input("Enter the name of the blob to store your data: ")
    blob = check_blob_existence(bucket, blob_name)
    if(blob is None):
        # probably need a try catch here
        blob = bucket.blob(blob_name) 
    
    # need to make this, especially the chunk_size more dynamic in the future
    # need checksum eventually
    blob.upload_from_filename(filename=input_data_path)

filepath = input("Enter the path to the file you want to upload data from: ")
add_to_storage(filepath)