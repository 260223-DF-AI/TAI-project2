# from fastapi import FastAPI, APIRouter, HTTPException

# # "/"
# # "/sales"
# router = APIRouter(
#     prefix="/sales",
#     tags=["sales"],
#     responses={404: {"description": "Not found"}}
# )

# # localhost:8000/sales/
# @router.get("/")
# def get_sales_root():
#     return {"message": "Hello from sales routes"}

# @router.get("/exception")
# def get_exception():
#     raise HTTPException(status_code=404, detail="Not found")
import pandas as pd
from fastapi import FastAPI, APIRouter, HTTPException
from pathlib import Path
import os

from src.util.file_reader import do_everything, load_data
from services.gc_storage import add_to_storage
from services.gc_bigquery import create_dataset, create_table
from services.env_vars import project_id

# router responsibly for querys about sales like money made
salesRouter = APIRouter(
    prefix="/sales",
    tags=["sales"]
)

productRouter = APIRouter(
    prefix="/products",
    tags=["products"]
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
    pass

@salesRouter.get("/{product_id}")
def get_sales_product(product_id: str):
    """
    Get the sales from a single product
    """
    pass

app = FastAPI()

app.include_router(salesRouter)
app.include_router(productRouter)
app.include_router(customerRouter)

@app.get("/")
def get_root():
    return "it works!"

@app.post("/")
def post_root():
    
    """data_folder = Path(__file__).resolve().parent.parent.parent / "data"
    files = [f for f in data_folder.iterdir() if f.is_file() and (f.name.endswith(".csv"))]

    # return files

    dfs = []
    invalid_df = []

    # sort through each file and validate
    for file in files:
        df, invalid = do_everything(file)
        # df = load_data(file)
        dfs.append(df)
        invalid_df.append(invalid)

    # combine all dataframes
    df = pd.concat(dfs)

    # convert to parquet file
    df.to_parquet("new_data/file.parquet", engine = 'pyarrow')
    

    #returns list of files"""
    #do_everything()
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