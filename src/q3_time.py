from typing import List, Tuple  # Importing List and Tuple types for type hints
from pipelines.queries import q3  # Importing specific query function from pipelines module
from pipelines.cstorage_and_bq import read_from_bigquery, cloud_storage_to_bigquery  # Importing functions for Cloud Storage and BigQuery operations
import os  # Importing os module for interacting with the operating system


# Read environment variables
CS_BUCKET_NAME = os.getenv("CS_BUCKET_NAME")
BQ_DATASET_ID = os.getenv("BQ_DATASET_ID")

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Executes a pipeline to load data from Cloud Storage to BigQuery and perform a query.

    Args:
        file_path (str): The path of the file in Cloud Storage.

    Returns:
        List[Tuple[datetime.date, str]]: A list of tuples containing date and string values.

    Raises:
        Exception: Raised if the data pipeline fails.
    """
    # Extract table_id from file_path (assuming the file_path is something like "table_name.json")
    table_id = file_path.split(".")[0]

    try:
        # Load data from Cloud Storage to BigQuery
        success = cloud_storage_to_bigquery(file_path, CS_BUCKET_NAME, BQ_DATASET_ID, table_id)
        
        if success:
            # Process result by querying BigQuery
            result = read_from_bigquery(BQ_DATASET_ID, table_id, q3)
            return result
        else:
            # If cloud_storage_to_bigquery fails, raise an exception with function name
            raise Exception(f"Failed in function 'q3_time': Cloud Storage to BigQuery pipeline failed.")
    
    except Exception as e:
        # Re-raise the exception with the function name included in the error message
        raise Exception(f"Error in function 'q3_time': {str(e)}")