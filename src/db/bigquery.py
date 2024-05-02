from google.cloud import bigquery
from utils.logging import get_logger
from dotenv import load_dotenv
import os

load_dotenv()

class BigQueryClient:
    def __init__(self):
        self.logger = get_logger(__name__)
        try:
            self.client = bigquery.Client()
            self.logger.info("BigQuery client successfully initialized.")
        except Exception as e:
            error_message = f"Error occurred during BigQuery client initialization: {e}"
            self.logger.error(error_message)
            raise  # Reraise the exception to indicate initialization failure

    def read_table(self, sql_query: str, dataset_id: str = "DE_BIGQUERY_LB", table_id: str = "test0006") -> None:
        """Execute a SQL query on a specified table in BigQuery.

        Args:
            dataset_id (str): The ID of the dataset containing the table.
            table_id (str): The ID of the table to query.
            sql_query (str): The SQL query to execute.

        Raises:
            google.api_core.exceptions.GoogleAPIError: If there's an error executing the query.
        """
        try:
            dataset_ref = self.client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)
            
            # Build the query job configuration
            query_job_config = bigquery.QueryJobConfig()

            # Execute the query
            query_job = self.client.query(sql_query, job_config=query_job_config)
            results = query_job.result()  # Wait for query to complete
            return results

        except Exception as e:
            error_message = f"Error occurred while executing SQL query on '{dataset_id}.{table_id}': {e}"
            self.logger.error(error_message)
            raise  # Reraise the exception to propagate the error
