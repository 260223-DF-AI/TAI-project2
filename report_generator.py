import os
from pathlib import Path
import time

from services.decorators import timers
from services.gc_bigquery import get_highest_unit_product, sales_total
from services.gc_storage import delete_bucket, initialize_sclient, add_to_storage
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
        "February": 2,
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

def print_report(report: dict):
    report_str = f"""

\033[1m\033[3mBENCHMARK REPORT\033[0m\033[0m

Disk space savings: \033[0;32m{report["Disk space savings"]:.2f}% saved with Parquet files compared to CSV\033[0m

Average (out of 10) time to upload to GCS:
    When file hashes \033[0;31mdo not exist\033[0m in bucket: {report["hash miss"]:.2f} seconds
    When file hashes \033[0;32mdo exist\033[0m in bucket: {report["hash hit"]:.2f} seconds

Average (out of 10) query access duration from BigQuery API:
    sales_total query: {report['sales_total']:.2f} seconds
    get_highest_unit_product query: {report['get_highest_unit_product']:.2f} seconds

"""
    print(report_str)

if __name__ == "__main__":
    # setup
    csv_folder = Path(__file__).resolve().parent / "data"
    parquet_folder = Path(__file__).resolve().parent / ".new_data"

    csv_files = [f for f in csv_folder.iterdir() if f.is_file() and (f.name.endswith(".csv"))]
    parquet_files = [f for f in parquet_folder.iterdir() if f.is_file() and (f.name.endswith(".parquet"))]

    empty_bucket_speed_list = []
    duplicate_speed_list = []
    sales_total_time = []
    highest_prodict_time = []

    # Calculate statistics for the report
    percent_saved = calculate_space_savings(csv_files, parquet_files)

    for i in range(10):
        # delete bucket to recreate
        delete_bucket('tai-project2-bucket')
        
        empty_bucket_speed, duplicate_speed = calculate_upload_time(parquet_files)
        empty_bucket_speed_list.append(empty_bucket_speed)
        duplicate_speed_list.append(duplicate_speed)

        sales_total_time.append(timers(sales_total)())
        highest_prodict_time.append(timers(get_highest_unit_product)())
    
    avg_empty_bucket_speed = sum(empty_bucket_speed_list) / 10
    avg_duplicate_speed = sum(duplicate_speed_list) / 10

    avg_sales_query_time = sum(sales_total_time) / 10
    avg_product_query_time = sum(highest_prodict_time) / 10

    # store result
    report_dict = {
        "Disk space savings": percent_saved,
        "hash miss": avg_empty_bucket_speed,
        "hash hit": avg_duplicate_speed,
        "sales_total": avg_sales_query_time,
        "get_highest_unit_product": avg_product_query_time
    }

    print_report(report_dict)
