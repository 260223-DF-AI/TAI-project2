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

DATA_FOLDER = Path(__file__).resolve().parent.parent / "data"


@app.get("/")
def get_root():
    return "it works!"

@app.post("/")
def post_root():
    files = list(DATA_FOLDER.glob("*"))
    
    # Just read the first file to print a few lines
    return files
    if files:
        first_file = files[0]
        print(f"Reading file: {first_file.name}")
        with open(first_file, "r") as f:
            # Read first 2 lines for testing
            lines = [next(f).strip() for _ in range(2)]
            print(lines)
        return {"message": f"Read first file: {first_file.name}", "lines": lines}
    else:
        return {"message": "No files found in data folder"}