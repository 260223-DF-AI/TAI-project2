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
from fastapi import FastAPI
from pathlib import Path

app = FastAPI()

#DATA_FOLDER = Path(__file__).resolve().parent.parent / "data"


@app.get("/")
def get_root():
    return "it works!"

@app.post("/")
def post_root():
    
    data_folder = Path("../../data")
    files = [f for f in data_folder.iterdir() if f.is_file() and (f.name.endswith(".csv"))]

    #returns list of files
    return files