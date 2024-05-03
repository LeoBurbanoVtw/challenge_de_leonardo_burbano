import gdown

class GoogleDriveClient:
    """
    A class to download files from Google Drive using a shared link.

    Attributes:
        url (str): The direct download URL of the Google Drive file.
        output_path (str): The path to save the downloaded file.
    """

    def __init__(self, url, output_path):
        """
        Initialize the GoogleDriveDownloader instance.

        Args:
            url (str): The direct download URL of the Google Drive file.
            output_path (str): The path to save the downloaded file.
        """
        self.url = url
        self.output_path = output_path

    def download_file(self):
        """
        Download the file from Google Drive.

        Raises:
            Exception: If the download fails due to any reason.
        """
        try:
            # Download the file
            gdown.download(self.url, self.output_path, quiet=False)
            print(f"Downloaded file saved to: {self.output_path}")
        except Exception as e:
            # Handle any error that occurs during the download process
            print(f"Error downloading file: {e}")
            raise Exception("Download failed")

# Example usage:
# if __name__ == "__main__":
#     # Google Drive file URL (make sure it's the direct download link)
#     url = 'https://drive.google.com/uc?id=1ig2ngoXFTxP5Pa8muXo02mDTFexZzsis'

#     # Create an instance of GoogleDriveDownloader
#     downloader = GoogleDriveClient(url, "./tmp/downloaded_file.zip")

#     try:
#         # Attempt to download the file
#         downloader.download_file()
#     except Exception as e:
#         print(f"Failed to download file: {e}")
