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

from util.file_reader import do_everything, load_data

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


@app.get("/")
def get_root():
    return "it works!"

@app.post("/")
def post_root():
    
    data_folder = Path(__file__).resolve().parent.parent.parent / "data"
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
    