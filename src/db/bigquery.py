# Import statements with comments
from google.cloud import bigquery  # Import the BigQuery client library
from google.cloud.exceptions import NotFound  # Import NotFound exception from Google Cloud library
from utils.logging import get_logger  # Import custom logging function
from dotenv import load_dotenv  # Import load_dotenv function from dotenv library

# Load environment variables from a .env file if present
load_dotenv()

class BigQueryClient:
    """
    A class for interacting with Google BigQuery to create tables, load data, and execute SQL queries.
    """

    def __init__(self):
        """
        Initialize the BigQuery client and logger.
        """
        self.logger = get_logger(__name__)  # Initialize logger using custom function
        try:
            self.client = bigquery.Client()  # Initialize BigQuery client
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
            query_job_config = bigquery.QueryJobConfig()  # Initialize query job configuration

            # Execute the query with string replacement (avoid SQL injection)
            query_job = self.client.query(
                sql_query.format(dataset_id=dataset_id, table_id=table_id),
                job_config=query_job_config
            )
            results = query_job.result()  # Wait for query to complete
            return results

        except Exception as e:
            error_message = f"Error occurred while executing SQL query on '{dataset_id}.{table_id}': {e}"
            self.logger.error(error_message)
            raise  # Reraise the exception to propagate the error
