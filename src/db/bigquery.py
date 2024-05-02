from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from utils.logging import get_logger
from dotenv import load_dotenv
import os

load_dotenv()

class BigQueryClient:
    """
    A class for interacting with Google BigQuery to create tables, load data, and execute SQL queries.
    """

    def __init__(self):
        """
        Initialize the BigQuery client and logger.
        """
        self.logger = get_logger(__name__)
        try:
            self.client = bigquery.Client()
            self.logger.info("BigQuery client successfully initialized.")
        except Exception as e:
            error_message = f"Error occurred during BigQuery client initialization: {e}"
            self.logger.error(error_message)
            raise  # Reraise the exception to indicate initialization failure

    def create_table(self, dataset_id, table_id):
        """
        Create a BigQuery table or delete it if it already exists.

        Args:
            dataset_id (str): The ID of the dataset.
            table_id (str): The ID of the table to create or delete.

        Returns:
            google.cloud.bigquery.table.TableReference: Reference to the created or existing table.
        """
        try:
            dataset_ref = self.client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)

            # Delete the table if it already exists
            try:
                self.client.get_table(table_ref)
                self.client.delete_table(table_ref)
                self.logger.info(f"Existing table {dataset_id}.{table_id} deleted.")
            except NotFound:
                self.logger.info(f"Table {dataset_id}.{table_id} does not exist yet.")

            return table_ref

        except Exception as e:
            error_message = f"Error occurred while creating or deleting table '{dataset_id}.{table_id}': {e}"
            self.logger.error(error_message)
            raise  # Reraise the exception to propagate the error

    def load_data_from_uri(self, table_ref, file_uri):
        """
        Load data from a Cloud Storage URI into a BigQuery table with schema auto-detection.

        Args:
            table_ref (google.cloud.bigquery.table.TableReference): Reference to the destination table.
            file_uri (str): The Cloud Storage URI of the data file.

        Raises:
            google.cloud.exceptions.GoogleCloudError: If there's an error during the data loading process.
        """
        try:
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.autodetect = True  # Enable schema auto-detection

            load_job = self.client.load_table_from_uri(
                file_uri, table_ref, job_config=job_config
            )

            self.logger.info(f"Starting job {load_job.job_id}")
            load_job.result()  # Wait for the job to complete
            self.logger.info(f"Job {load_job.job_id} completed successfully.")

        except Exception as e:
            error_message = f"Error occurred while loading data into '{table_ref.dataset_id}.{table_ref.table_id}': {e}"
            self.logger.error(error_message)
            raise  # Reraise the exception to propagate the error

    def read_table(self, sql_query: str, dataset_id: str = "DE_BIGQUERY_LB", table_id: str = "test0006") -> bigquery.table.RowIterator:
        """
        Execute a SQL query on a specified table in BigQuery.

        Args:
            sql_query (str): The SQL query to execute.
            dataset_id (str, optional): The ID of the dataset containing the table. Defaults to "DE_BIGQUERY_LB".
            table_id (str, optional): The ID of the table to query. Defaults to "test0006".

        Returns:
            google.cloud.bigquery.table.RowIterator: Iterator over the query results.

        Raises:
            google.api_core.exceptions.GoogleAPIError: If there's an error executing the query.
        """
        try:

            # Build the query job configuration
            query_job_config = bigquery.QueryJobConfig()
            
            regex_pattern = '''
            r"(?:[\U0001F300-\U0001F5FF]|[\U0001F900-\U0001F9FF]|[\U0001F600-\U0001F64F]|[\U0001F680-\U0001F6FF]|[\U00002600-\U000026FF]\uFE0F?|[\U00002700-\U000027BF]\uFE0F?|\u24C2\uFE0F?|[\U0001F1E6-\U0001F1FF]{1,2}|[\U0001F170\U0001F171\U0001F17E\U0001F17F\U0001F18E\U0001F191-\U0001F19A]\uFE0F?|[\u0023\u002A\u0030-\u0039]\uFE0F?\u20E3|[\u2194-\u2199\u21A9-\u21AA]\uFE0F?|[\u2B05-\u2B07\u2B1B\u2B1C\u2B50\u2B55]\uFE0F?|[\u2934\u2935]\uFE0F?|[\u3297\u3299]\uFE0F?|[\U0001F201-\U0001F202\U0001F21A\U0001F22F\U0001F232\U0001F23A\U0001F250\U0001F251]\uFE0F?|[\u203C-\u2049]\uFE0F?|[\u00A9-\u00AE]\uFE0F?|[\u2122\u2139]\uFE0F?|\U0001F004\uFE0F?|\U0001F0CF\uFE0F?|[\u231A\u231B\u2328\u23CF\u23E9\u23F3\u23F8\u23FA]\uFE0F?)"
            '''
            # Execute the query
            query_job = self.client.query(sql_query.format(dataset_id=dataset_id, table_id=table_id, regex_pattern= regex_pattern), job_config=query_job_config)
            results = query_job.result()  # Wait for query to complete
            return results

        except Exception as e:
            error_message = f"Error occurred while executing SQL query on '{dataset_id}.{table_id}': {e}"
            self.logger.error(error_message)
            raise  # Reraise the exception to propagate the error