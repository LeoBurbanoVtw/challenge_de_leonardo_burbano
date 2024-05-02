from typing import List, Tuple
from datetime import datetime
from db.bigquery import BigQueryClient
from utils.queries import q3

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    bigquery_client = BigQueryClient()
    result = bigquery_client.read_table(sql_query=q3)
    list_output = [(row[0], row[1]) for row in result]
    return list_output