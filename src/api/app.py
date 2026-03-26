import pandas as pd
from fastapi import FastAPI, APIRouter, HTTPException
from pathlib import Path
import os

from src.util.file_reader import do_everything, load_data
from services.gc_storage import add_to_storage
from services.gc_bigquery import *
from services.env_vars import project_id

# router responsibly for querys about sales like money made
salesRouter = APIRouter(
    prefix="/sales",
    tags=["sales"]
)

productRouter = APIRouter(
    prefix="/product",
    tags=["product"]
)

customerRouter = APIRouter(
    prefix="/customers",
    tags=["customers"]
)

@salesRouter.get("/")
def get_sales_root():
    pass

@salesRouter.get("/total")
def get_sales_total():
    """
    Get the total amount of sales
    """
    df = sales_total()
    json_compatible_data = df.to_dict(orient="records")
    return json_compatible_data


@salesRouter.get("/{store_id}")
def get_sales_total_by_product(store_id: str):
    """
    Get the highest total from a single store
    """
    df = get_sales_total_by_store(store_id)
    json_compatible_data = df.to_dict(orient="records")
    return json_compatible_data

@productRouter.get("/highest_quantity")
def get_highest_quantity_product():
    """
    Get the product that has the most units sold
    """
    df = get_highest_unit_product()
    json_compatible_data = df.to_dict(orient="records")
    return json_compatible_data

@productRouter.get("/highest_quantity/{month}")
def get_highest_quantity_by_month(month: int):
    """
    Get the product that has the most units sold in a particular month
    """
    df = get_highest_unit_product_month(month)
    json_compatible_data = df.to_dict(orient="records")
    return json_compatible_data

@customerRouter.get("/customer_report/{customer_id}")
def get_customer_report(customer_id: str):
    """
    Get the report on a specific customer including their name, how many transaction they make, and total amount spent
    """
    df = get_customer_summary(customer_id)
    json_compatible_data = df.to_dict(orient="records")
    return json_compatible_data


@productRouter.get("/")
def get_products_root():
    pass


app = FastAPI()

@app.get("/")
def get_root():
    return "it works!"

@app.post("/")
def post_root():
    """
    Loads csv, Converts csv files to partitioned parquets, Sends parquets to Google Cloud Storage, 
    and creates table and dataset if they don't already exist
    """
    # do_everything()
    data_folder = Path(__file__).resolve().parent.parent.parent / ".new_data"
    files = [f for f in data_folder.iterdir() if f.is_file() and (f.name.endswith(".parquet"))]
    month_name_to_number = {
        "January": 1,
        "Febraury": 2,
        "March": 3,
        "April": 4,
        "May": 5,
        "June": 6,
        "July": 7,
        "August": 8,
        "September": 9,
        "October": 10,
        "November": 11,
        "December": 12
    }
    for file in files:
        name = os.path.splitext(os.path.basename(file))[0]
        add_to_storage(file, "sales_data", { "year": "2025", "month": month_name_to_number[name]})
    create_dataset(f'{project_id}.tai_cloud_project_dataset')
    create_table('tai_cloud_project_dataset', 'transactions')


app.include_router(salesRouter)
app.include_router(productRouter)
app.include_router(customerRouter)