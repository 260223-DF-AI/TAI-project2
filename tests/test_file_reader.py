import pandas as pd
import pytest as pyt
from pathlib import Path

from src.util.file_reader import load_data, validate, clean

# Get the directory where this specific test file lives
BASE_DIR = Path(__file__).resolve().parent

# Build the path relative to this file
# This goes up one level to the project root, then into 'data'
DATA_PATH = BASE_DIR / ".." / "data" / "dummy_sales_batch_1.csv"

TEST_DIR = Path(__file__).resolve().parent

TEST_PATH = TEST_DIR / "test.csv"

df = load_data(DATA_PATH)
test = load_data(TEST_PATH)

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
    pass

def test_validate():
    valid, invalid = validate(test, 10000)
    
    assert not valid.empty
