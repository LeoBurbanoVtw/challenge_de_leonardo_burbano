# Import statements with comments
import gdown  # Import the gdown library for downloading files
from utils.logging import get_logger  # Import custom logging function


class GoogleDriveClient:
    """
    A class to download files from Google Drive using a shared link.

    Attributes:
        url (str): The direct download URL of the Google Drive file.
        output_path (str): The path to save the downloaded file.
    """

    def __init__(self, url, output_path):
        """
        Initialize the GoogleDriveClient instance.

        Args:
            url (str): The direct download URL of the Google Drive file.
            output_path (str): The path to save the downloaded file.
        """
        self.url = url
        self.output_path = output_path
        self.logger = get_logger(__name__)  # Initialize logger using custom function

    def download_file(self):
        """
        Download the file from Google Drive.

        Raises:
            Exception: If the download fails due to any reason.
        """
        try:
            # Download the file using gdown library
            gdown.download(self.url, self.output_path, quiet=False)
            self.logger.info(f"Downloaded file saved to: {self.output_path}")
        except Exception as e:
            # Log error that occurs during the download process
            self.logger.error(f"Error downloading file: {e}")
            raise Exception("Download failed")
