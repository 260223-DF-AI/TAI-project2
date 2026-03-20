from google.cloud import bigquery

class BigqueryService:
    query_client: bigquery.Client = bigquery.Client()
