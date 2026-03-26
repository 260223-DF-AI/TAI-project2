import os
from pathlib import Path
import time

from services.decorators import timers
from services.gc_bigquery import get_highest_unit_product, sales_total
from services.gc_storage import initialize_sclient, add_to_storage
from src.util.file_reader import do_everything


def calculate_space_savings(csv_files: list, parquet_files: list):
    csv_size = 0
    parquet_size = 0

    # compute total size of the csv files in kilobytes
    for file in csv_files:
        csv_size += (os.path.getsize(file) / 1000) 
    
    # compute total size of the parquet files in kilobytes
    for file in parquet_files:
        parquet_size += (os.path.getsize(file) / 1000)

    return ((csv_size - parquet_size) / csv_size) * 100

def calculate_upload_time(files: list):
    # setup
    initialize_sclient()

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

    # calculate time to upload everything, when our bucket is empty
    start = time.time()
    for file in files:
        name = os.path.splitext(os.path.basename(file))[0]
        add_to_storage(file, "sales_data", { "year": "2025", "month": month_name_to_number[name]})
    empty_bucket_upload_speed = time.time() - start

    # calculate time when trying to upload duplicate bundles
    start = time.time()
    for file in files:
        name = os.path.splitext(os.path.basename(file))[0]
        add_to_storage(file, "sales_data", { "year": "2025", "month": month_name_to_number[name]})
    duplicate_upload_speed = time.time() - start

    return empty_bucket_upload_speed, duplicate_upload_speed

if __name__ == "__main__":
    # setup
    csv_folder = Path(__file__).resolve().parent / "data"
    parquet_folder = Path(__file__).resolve().parent / ".new_data"

    csv_files = [f for f in csv_folder.iterdir() if f.is_file() and (f.name.endswith(".csv"))]
    parquet_files = [f for f in parquet_folder.iterdir() if f.is_file() and (f.name.endswith(".parquet"))]

    do_everything()

    percent_saved = calculate_space_savings(csv_files, parquet_files)
    empty_bucket_speed, duplicate_speed = calculate_upload_time(parquet_files)

    sales_total_time = timers(sales_total)()
    highest_prodict_time = timers(get_highest_unit_product)()

    report_dict = {
        "Disk space savings": percent_saved,
        "Upload speed to an empty bucket": empty_bucket_speed,
        "Upload speed, trying to upload duplicate files": duplicate_speed,
        "Sales_total query time": sales_total_time,
        "get_highest_unit_product query time": highest_prodict_time
    }

    print(report_dict)
