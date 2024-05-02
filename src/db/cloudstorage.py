from google.cloud import storage
from google.cloud.exceptions import NotFound

class CloudStorageClient:
    """
    A class for interacting with Google Cloud Storage (GCS) to retrieve file URIs.
    """

    def __init__(self):
        """
        Initializes the CloudStorageClient with a Google Cloud Storage client.
        """
        self.client = storage.Client()

    def get_file_uri(self, bucket_name, file_name):
        """
        Retrieves the Cloud Storage URI for a given file in a specified bucket.

        Args:
            bucket_name (str): The name of the Google Cloud Storage bucket.
            file_name (str): The name of the file within the bucket.

        Returns:
            str or None: The Cloud Storage URI (gs://bucket_name/file_name) if the file exists,
                         None if the file is not found or if any errors occur.
        """
        try:
            # Get the bucket object
            bucket = self.client.get_bucket(bucket_name)

            # Get the blob (file) object within the bucket
            blob = bucket.blob(file_name)

            # Check if the blob (file) exists in the bucket
            if blob.exists():
                file_uri = f"gs://{bucket_name}/{file_name}"
                return file_uri
            else:
                print(f"File {file_name} not found in bucket {bucket_name}.")
                return None
        except NotFound:
            # Handle the case where the bucket does not exist
            print(f"Bucket {bucket_name} not found.")
            return None