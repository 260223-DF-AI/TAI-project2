from google.cloud import bigquery
from services.decorators import app_logger, log_to_app
from services.env_vars import project_id
from google.api_core.exceptions import GoogleAPIError, BadRequest, NotFound, Forbidden
import os
from re import match
import sys
from pathlib import Path

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent  # goes up from tests/ -> src/
services_path = project_root / "services"
sys.path.insert(0, str(services_path))

# Now imports like `from decorators import logger, app_logger` will work
import gc_bigquery as bq

"""
These functions use some of our limited amount of calls on GCP, so beware
"""

# def test_get_sales_total_by_store():
#     df = bq.get_sales_total_by_store("S002")
#     assert df["total_amount"].iloc[0] == 391939202.27

# def test_sales_total():
#     df = bq.get_sales_total()
#     assert df["total_amount"].iloc[0] == 3154347681.8399925

# def test_get_sales_total_by_product():
#     df = bq.get_sales_total_by_product("P103")
#     assert df["total_amount"].iloc[0] == 235148204.4999999

# def test_get_highest_unit_product():
#     df = bq.get_highest_unit_product() 
#     assert df["quantity"].iloc[0] == 841244.0

# def test_get_highest_unit_product_month():
#     df = bq.get_highest_unit_product_month(5)
#     assert df["quantity"].iloc[0] == 71509.0

# def test_get_customer_summary():
#     df = bq.get_customer_summary("C002")
#     assert df["total_amount"].iloc[0] == 394665969.9999999
