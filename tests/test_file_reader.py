import pandas as pd
import pytest as pyt
from pathlib import Path

from src.util.file_reader import load_data, validate, clean, create_parquet

# Get the directory where this specific test file lives
BASE_DIR = Path(__file__).resolve().parent

# Build the path relative to this file
# This goes up one level to the project root, then into 'data'
DATA_PATH = BASE_DIR / ".." / "data" / "dummy_sales_batch_1.csv"

TEST_DIR = Path(__file__).resolve().parent

TEST_PATH = TEST_DIR / "test_files"

df = load_data(DATA_PATH)
test = load_data(TEST_PATH/"test.csv")

def test_load_data():

    # Act + Assert 
    assert not df.empty
    assert not test.empty
    
    # Arrange
    columns = [
        "TransactionID", "Date", "StoreID", "StoreLocation", "Region", 
        "State", "CustomerID", "CustomerName", "Segment", "ProductID", 
        "ProductName", "Category", "SubCategory", "Quantity", "UnitPrice", 
        "DiscountPercent", "TaxAmount", "ShippingCost", "TotalAmount"
    ]

    # Act + Assert correct file
    assert list(df.columns) == columns
    assert list(test.columns) == columns

def test_clean():
    # checks for nulls
    new_file = test.copy()
    clean(new_file)
    assert new_file.isnull().sum().sum() == 0

    # checks that it succesfully removed a duplicate
    duplicate_count = (new_file['TransactionID'] == '11').sum()
    assert duplicate_count == 1

def test_validate():
    # Arrange
    valid, invalid = validate(test, 10000)

    # check that invalid data is correct
    invalid_df = pd.read_csv(TEST_PATH/"invalid.csv")
    assert invalid_df.shape == invalid.shape

    # check that valid data is correct
    valid_df = pd.read_csv(TEST_PATH/"valid.csv")
    assert valid_df.shape == valid.shape

    # checks valid data is all correct type
    number_cols = [ 
        'TransactionID', 'Quantity', 'UnitPrice',
        'DiscountPercent', 'TaxAmount', 
        'ShippingCost', 'TotalAmount'
    ]

    for field in number_cols:
        assert valid[field].dtype in ["int64", "float64"]

def test_create_parquet():
    df = load_data(DATA_PATH)
    create_parquet("file", df)
    assert Path(".new_data/file.parquet").is_file()  
    # clean up
    Path(".new_data/file.parquet").unlink()