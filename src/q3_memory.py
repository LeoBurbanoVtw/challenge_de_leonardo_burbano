from typing import List, Tuple
from datetime import datetime
from db.bigquery import BigQueryClient
from utils.queries import q3
from memory_profiler import profile

@profile
def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    bigquery_client = BigQueryClient()
    result = bigquery_client.read_table(sql_query=q3)
    list_output = [(row[0], row[1]) for row in result]
    return list_output