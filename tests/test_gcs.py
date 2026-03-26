import pandas as pd
import pytest as pyt
import sys
from pathlib import Path
from google.cloud import storage
from unittest.mock import MagicMock, patch
# -------------------------------
# Add src/services to sys.path so imports in gc_storage.py work
# -------------------------------
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent  # goes up from tests/ -> src/
services_path = project_root / "src" / "services"
sys.path.insert(0, str(services_path))

# Now imports like `from decorators import logger, app_logger` will work
import gc_storage as gcs

def test_initialize_sclient():
    gcs.initialize_sclient()
    assert isinstance(gcs.storage_client, storage.Client)


@patch("mymodule.storage_client")
def test_check_bucket_existence_found(mock_storage_client):
    # create fake buckets
    bucket1 = MagicMock()
    bucket1.name = "bucket-a"

    bucket2 = MagicMock()
    bucket2.name = "bucket-b"

    # mock list_buckets
    mock_storage_client.list_buckets.return_value = [bucket1, bucket2]

    result = gcs.check_bucket_existence("bucket-b")

    assert result == bucket2


"""
These functions use some of our limited amount of calls on GCP, so beware
"""

# def test_check_bucket_existence():
#     badBucket = gcs.check_bucket_existence("fake_bucket")
#     goodBucket = gcs.check_bucket_existence('tai-project2-bucket')
#     assert badBucket is None
#     assert goodBucket is not None

# def test_check_blob_existence():
#     badBlob = gcs.check_blob_existence(gcs.check_bucket_existence("tai-project2-bucket"), "fake_blob")
#     goodBlob = gcs.check_blob_existence(gcs.check_bucket_existence("tai-project2-bucket"), "sales_data/year=2025/month=1/January.parquet")
#     assert badBlob is None
#     assert goodBlob is not None

# def teast_crc_hash_exists():
#     todo

# def test_add_to_storage():
#     todo