import zipfile # To upzip files in disk
import os # To access OS

def unzip_file(zip_path: str, output_dir: str) -> str:
    """
    Unzips a .zip file containing a single file to disk.

    Args:
        zip_path (str): Path to the .zip file.
        output_dir (str): Directory where the extracted file should be written.

    Returns:
        str or None: Filename of the extracted file if successfully extracted,
                     or None if the input data is not a valid .zip file or contains
                     more than one file.

    Raises:
        FileNotFoundError: If the specified zip file does not exist.
        zipfile.BadZipFile: If the data is not a valid zip file.
        ValueError: If the zip file does not contain exactly one file.
    """
    try:
        # Check if the specified zip file exists
        if not os.path.exists(zip_path):
            raise FileNotFoundError(f"Error: Zip file '{zip_path}' not found.")

        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Remove existing .json files in the output directory
        for file_name in os.listdir(output_dir):
            if file_name.endswith('.json'):
                file_path = os.path.join(output_dir, file_name)
                os.remove(file_path)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Ensure there's exactly one file in the zip archive
            if len(zip_ref.infolist()) != 1:
                raise ValueError("Error: The zip file does not contain exactly one file.")

            # Extract the single file to the specified output directory
            extracted_file_info = zip_ref.infolist()[0]
            extracted_file_path = zip_ref.extract(extracted_file_info, path=output_dir)

            # Get the filename of the extracted file
            extracted_filename = os.path.basename(extracted_file_path)

            return extracted_filename

    except FileNotFoundError as e:
        print(e)
        return None

    except zipfile.BadZipFile:
        # If the data is not a valid zip file
        print("Error: Provided data is not a valid .zip file.")
        return None

    except ValueError as e:
        print(e)
        return None