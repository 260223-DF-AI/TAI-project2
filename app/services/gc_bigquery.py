from google.cloud import bigquery
from env_vars import project_id

query_client: bigquery.Client = bigquery.Client(project=project_id)
