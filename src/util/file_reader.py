import pandas as pd
import pyarrow as pa
from pathlib import Path

def load_data(filepath):
    """
    Creates a DataFrame object from the given file, cleans it, then returns it
    """
    df = pd.DataFrame()
    # dotIndex = filepath.rfind('.')
    try:
        # if(filepath[dotIndex + 1:].lower() == 'csv'):
        df = pd.read_csv(filepath)
        # else:
            # raise WrongFileType (create this exception later)
    except FileNotFoundError:
        print("File Note Found") # replace with logger
    return df

def clean(df):
    # drop null
    df.dropna()
    # drop duplicates
    df.drop_duplicates(inplace=True)

def validate(df, chunk_size=100_000):
    number_cols = [
        'TransactionID','Quantity', 'UnitPrice',
        'DiscountPercent', 'TaxAmount', 'ShippingCost', 'TotalAmount'
    ]

    valid_chunks = []
    invalid_chunks = []

    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size].copy()

        # Convert numeric columns
        chunk[number_cols] = chunk[number_cols].apply(
            pd.to_numeric, errors='coerce'
        )

        # Convert date
        chunk['Date'] = pd.to_datetime(
            chunk['Date'],
            format="%Y-%m-%d",
            errors='coerce'
        )
        # Build validation mask
        valid_mask = (
            (chunk['TransactionID'] > 0) &
            (chunk[number_cols[1:]] >= 0).all(axis=1) & # all numeric >= 0
            (chunk['Date'].notna())  # valid datetime
        )

        valid_chunks.append(chunk[valid_mask])
        invalid_chunks.append(chunk[~valid_mask])

    valid_df = pd.concat(valid_chunks, ignore_index=True)
    invalid_df = pd.concat(invalid_chunks, ignore_index=True)

    return valid_df, invalid_df

def create_parquet(filename, df):
    """
    Turn the given Dataframe object into a parquet file, then load them into new_data folder
    """
    path = Path(__file__).resolve().parent.parent.parent / '.new_data'
    df.to_parquet(path / f"{filename}.parquet", engine = "pyarrow")

def do_everything():
    
    data_folder = Path(__file__).resolve().parent.parent.parent / "data"
    files = [f for f in data_folder.iterdir() if f.is_file() and (f.name.endswith(".csv"))]
    megaDf = pd.DataFrame()
    megaInvalid = pd.DataFrame()

    for file in files:
        df = load_data(file)
        valid, invalid = validate(df, 25000) # type: ignore        
        megaDf = pd.concat([megaDf, valid])
        megaInvalid = pd.concat([megaInvalid, invalid])

    monthDict = {
        "January": [],
        "February": [],
        "March": [],
        "April": [],
        "May": [],
        "June": [],
        "July": [],
        "August": [],
        "September": [],
        "October": [],
        "November": [],
        "December": []
    }

    for i, month in enumerate(monthDict):
        monthDict[month] = megaDf[megaDf["Date"].dt.month == i + 1]
        create_parquet(month, monthDict[month])