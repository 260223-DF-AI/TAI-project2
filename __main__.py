import pandas as pd
import pyarrow
from pathlib import Path

from util.file_reader import do_everything

# def main():
#     print("Begin Loading Data...")

#     # Load each csv file
#     df1, invalid1 = do_everything("data/dummy_sales_batch_1.csv")
#     df2, invalid2 = do_everything("data/dummy_sales_batch_2.csv")
#     df3, invalid3 = do_everything("data/dummy_sales_batch_3.csv")
#     df4, invalid4 = do_everything("data/dummy_sales_batch_4.csv")
#     df5, invalid5 = do_everything("data/dummy_sales_batch_1.csv")

#     # combine all dataframes
#     df = pd.concat([df1, df2, df3, df4, df5])
#     print("Successfully combined valid data")

#     df.to_parquet("new_data/file.parquet", engine = 'pyarrow')
#     print("Successfully to parquet")

def main():
    print("Begin Loading Data...")

    data_folder = Path(__file__).resolve().parent / "data"
    files = [f for f in data_folder.iterdir() if f.is_file() and (f.name.endswith(".csv"))]

    # Load each csv file
    dfs = []
    invalid_df = []

    # sort through each file and validate
    for file in files:
        partialDf = do_everything(file)
        dfs.append(partialDf)
        #invalid_df.append(invalid)

    # combine all dataframes
    df = pd.concat(dfs)

    print("Successfully combined valid data")

    transactions_df = df[["TransactionID", "Date", "CustomerID", "ProductID", "Quantity", "DiscountPercent", "TaxAmount", "ShippingCost", "TotalAmount"]].copy()
    customer_df = df[["CustomerID", "CustomerName", "Segment"]].copy()
    store_df = df[["StoreID", "StoreLocation", "Region", "State"]].copy()
    product_df = df[["ProductID", "ProductName", "Category", "SubCategory", "UnitPrice"]].copy()

    transactions_df = transactions_df.drop_duplicates(subset = ["TransactionID"])
    customer_df = customer_df.drop_duplicates(subset = ["CustomerID"])
    store_df = store_df.drop_duplicates(subset = ["StoreID"])
    product_df = product_df.drop_duplicates(subset = ["ProductID"])

    transactions_df.to_parquet("new_data/transactions.parquet", engine = 'pyarrow')
    customer_df.to_parquet("new_data/customer.parquet", engine = 'pyarrow')
    store_df.to_parquet("new_data/store.parquet", engine = 'pyarrow')
    product_df.to_parquet("new_data/product.parquet", engine = 'pyarrow')

if __name__ == '__main__':
    main()