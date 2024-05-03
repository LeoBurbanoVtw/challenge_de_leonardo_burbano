from google.cloud import storage
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv

load_dotenv()

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
        
    def upload_to_gcs(self, file_path, bucket_name, destination_blob_name):
        """
        Uploads a file from a specified path to Google Cloud Storage (GCS) bucket.

        Args:
            file_path (str): The path to the file to upload.
            bucket_name (str): The name of the GCS bucket where the file will be uploaded.
            destination_blob_name (str): The name of the blob (object) in the bucket.

        Returns:
            str or None: The GCS URI of the uploaded file if successful,
                        or None if an error occurs during upload.
        """
        try:
            # Read file contents
            with open(file_path, "rb") as file:
                file_contents = file.read()

            # Upload the file contents to Google Cloud Storage
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(file_contents)

            # Return the GCS URI of the uploaded file
            return f"gs://{bucket_name}/{destination_blob_name}"

        except Exception as e:
            # If an error occurs during upload
            print(f"Error uploading to GCS: {e}")
            return None
