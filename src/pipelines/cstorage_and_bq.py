from typing import List, Tuple
from datetime import datetime, date
from db.bigquery import BigQueryClient
from db.cloudstorage import CloudStorageClient

# Initialize BigQuery and Cloud Storage clients
bigquery_client = BigQueryClient()
storage_client = CloudStorageClient()

def cloud_storage_to_bigquery(file_path: str, bucket_name: str, bq_dataset_id: str, bq_table_id: str) -> bool:
    """
    Load data from Cloud Storage into BigQuery.

    Args:
        file_path (str): The path of the file in Cloud Storage.
        bucket_name (str): The name of the Cloud Storage bucket.
        bq_dataset_id (str): The ID of the BigQuery dataset.
        bq_table_id (str): The ID of the BigQuery table to create and load data into.

    Returns:
        bool: True if the data was successfully loaded into BigQuery, False otherwise.

    Raises:
        Exception: Raised if there are any errors during the data loading process.
    """
    try:
        # Get the file URI from Cloud Storage
        file_uri = storage_client.get_file_uri(bucket_name, file_path)
        
        if file_uri:
            # Create the BigQuery table
            table_ref = bigquery_client.create_table(bq_dataset_id, bq_table_id)
            
            # Load data from the file URI into BigQuery
            bigquery_client.load_data_from_uri(table_ref, file_uri)
            return True
        else:
            # File URI not found
            return False
    
    except Exception as e:
        # Error occurred during data loading process
        raise Exception(f"Error in cloud_storage_to_bigquery: {str(e)}")

def read_from_bigquery(bq_dataset_id: str, bq_table_id: str, bq_sql_query: str) -> List[Tuple[any, any]]:
    """
    Read data from a BigQuery table based on a SQL query.

    Args:
        bq_dataset_id (str): The ID of the BigQuery dataset.
        bq_table_id (str): The ID of the BigQuery table.
        bq_sql_query (str): The SQL query to execute.

    Returns:
        List[Tuple[date, str]]: A list of tuples containing date and string values retrieved from BigQuery.

    Raises:
        Exception: Raised if there are any errors during the data retrieval process.
    """
    try:
        # Read data from BigQuery using the specified SQL query
        result = bigquery_client.read_table(dataset_id=bq_dataset_id, table_id=bq_table_id, sql_query=bq_sql_query)
        
        # Transform the result into a list of tuples (date, str)
        list_output: List[Tuple[any, any]] = [(row[0], row[1]) for row in result]
        
        return list_output
        
    except Exception as e:
        # Error occurred during data retrieval
        raise Exception(f"Error in read_from_bigquery: {str(e)}")