import os
from typing import Optional
from db.cloudstorage import CloudStorageClient
from db.googledrive import GoogleDriveClient

from utils.unzip import unzip_file
from utils.transform_json import transform_json

def gdrive_to_cstorage(url: str, zip_file_path: str, input_directory: str, output_directory: str) -> Optional[str]:
    """
    Extracts a file from a ZIP archive located at `zip_file_path` and uploads it to Google Cloud Storage.

    Args:
        url (str): URL of the Google Drive file to download.
        zip_file_path (str): Path to the ZIP file.
        output_directory (str): Directory where the extracted file will be stored.

    Returns:
        Optional[str]: The URI of the uploaded file in Google Cloud Storage if successful,
                       or None if any error occurs during the extraction or upload process.

    Raises:
        FileNotFoundError: If the specified ZIP file or extracted file is not found.
        Exception: For any other unexpected error during the upload process.
    """

    try:
        # Validate input paths before starting any operations
        if not os.path.exists(zip_file_path):
            raise FileNotFoundError(f"Error: ZIP file '{zip_file_path}' not found.")

        # Initialize Google Drive client to download the file
        downloader = GoogleDriveClient(url, zip_file_path)
        downloader.download_file()

        # Extract the ZIP file to the specified output directory
        extracted_filename = unzip_file(zip_file_path, input_directory)

        if not extracted_filename:
            raise FileNotFoundError("Error: No files extracted from the ZIP archive.")

        # Construct the path to the extracted file
        extracted_file_path = os.path.join(input_directory, extracted_filename)
        output_file_path = os.path.join(output_directory, extracted_filename)

        if not os.path.exists(extracted_file_path):
            raise FileNotFoundError(f"Error: Extracted file '{extracted_file_path}' not found.")
        
        # Transform file into a valid JSON
        is_json_transformed = transform_json(extracted_file_path, output_file_path)

        if not is_json_transformed:
            raise FileNotFoundError(f"Error: Extracted file '{extracted_file_path}' was not successfully transformed to JSON.")


        # Get Google Cloud Storage details from environment variables
        bucket_name = os.getenv("CS_BUCKET_NAME")
        destination_blob_name = os.path.basename(output_file_path)

        # Upload the extracted file to Google Cloud Storage
        cs_client = CloudStorageClient()
        uploaded_uri = cs_client.upload_to_gcs(output_file_path, bucket_name, destination_blob_name)

        if uploaded_uri:
            print(f"File uploaded to GCS. URI: {uploaded_uri}")
            return uploaded_uri
        else:
            raise Exception("Failed to upload file to GCS.")

    except FileNotFoundError as file_not_found_error:
        # Catch and re-raise FileNotFoundError with specific error message
        raise FileNotFoundError(file_not_found_error)

    except Exception as upload_error:
        # Catch and re-raise any unexpected exceptions during the upload process
        raise Exception(f"Error occurred during upload to GCS: {upload_error}")

    # Note: If no return or raised exception occurs within the try block,
    # the function will return None as per the function signature (Optional[str])


# Example usage:
# if __name__ == "__main__":
#     gdrive_url = "https://drive.google.com/file/1234567890"
#     zip_file_path = "/path/to/your/zipfile.zip"
#     output_dir = "/path/to/output/directory"

#     try:
#         uploaded_uri = gdrive_to_cstorage(gdrive_url, zip_file_path, output_dir)
#         if uploaded_uri:
#             print("Upload successful!")
#         else:
#             print("Upload failed.")
#     except Exception as e:
#         print(f"Error: {e}")
