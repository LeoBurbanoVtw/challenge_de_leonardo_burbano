from typing import List, Tuple
from datetime import datetime
from db.bigquery import BigQueryClient
from utils.queries import q1

def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    bigquery_client = BigQueryClient()
    result = bigquery_client.read_table(sql_query=q1)
    list_output = [(row[1], row[0]) for row in result]
    return list_output