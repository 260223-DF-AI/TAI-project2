import pandas as pd
import pyarrow
from util.file_reader import do_everything, load_data, validate

def main():
    print("Begin Loading Data...")

    
    df1, invalid1 = do_everything("data/dummy_sales_batch_1.csv")
    print(df1)
    """
    df2, invalid2 = do_everything("data/dummy_sales_batch_2.csv")
    df3, invalid3 = do_everything("data/dummy_sales_batch_3.csv")
    df4, invalid4 = do_everything("data/dummy_sales_batch_4.csv")
    df5, invalid5 = do_everything("data/dummy_sales_batch_1.csv")

    # combine all dataframes
    df = pd.concat([df1, df2, df3, df4, df5])
    print("Successfully combined valid data")

    df.to_parquet("new_data/file.parquet", engine = 'pyarrow')
    print("Successfully to parquet")"""


if __name__ == '__main__':
    main()