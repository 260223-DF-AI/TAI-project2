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

TEST_PATH = TEST_DIR / "test_files"

df = load_data(DATA_PATH)
test = load_data(TEST_PATH/"test.csv")

valid, invalid = validate(test, 10000)

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

def test_validate():
    # check that invalid data is correct
    invalid_df = pd.read_csv(TEST_PATH/"invalid.csv")
    pd.testing.assert_frame_equal(invalid_df, invalid)

    # check that valid data is correct
    valid_df = pd.read_csv(TEST_PATH/"valid.csv")
    pd.testing.assert_frame_equal(valid_df, valid)

    # checks valid data is all correct type
    valid["TransactionID"].dtype == "int"

    number_cols = [ 
        'Quantity', 'UnitPrice',
        'DiscountPercent', 'TaxAmount', 
        'ShippingCost', 'TotalAmount'
    ]

    for field in number_cols:
        valid[field].dtype in ["int64", "float64"]


def test_clean():
    # checks for nulls
    new_file = clean(test)
    assert new_file.isnull().sum().sum() == 0

    # checks that it succesfully removed a duplicate
    duplicate_count = (new_file['TransactionID'] == '11').sum()
    assert duplicate_count == 1
    

