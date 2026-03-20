from google.cloud import bigquery

query_client = bigquery.Client()
print(str(query_client))