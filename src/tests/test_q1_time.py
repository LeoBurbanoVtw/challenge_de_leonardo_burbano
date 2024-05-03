import pytest
from datetime import date  # Import the 'date' class from the 'datetime' module
import sys
import os

# Append the root folder to sys.path to ensure imports from src folder work correctly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from q1_time import q1_time


# Define fixture for environment variables
@pytest.fixture
def mock_environment_variables(monkeypatch):
    # Mock the environment variables
    monkeypatch.setenv("CS_BUCKET_NAME", "de_challenge_leonardo_burbano")
    monkeypatch.setenv("BQ_DATASET_ID", "DE_BIGQUERY_LB")
    monkeypatch.setenv("LOGS_FOLDER", "./logs/")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS","./db/de-leonardo-burbano-3f5eb87a186b.json")

# Define the expected result as a list of tuples containing (date, string)
expected_result = [
    (date(2021, 2, 12), 'RanbirS00614606'),
    (date(2021, 2, 13), 'MaanDee08215437'),
    (date(2021, 2, 17), 'RaaJVinderkaur'),
    (date(2021, 2, 16), 'jot__b'),
    (date(2021, 2, 14), 'rebelpacifist'),
    (date(2021, 2, 18), 'neetuanjle_nitu'),
    (date(2021, 2, 15), 'jot__b'),
    (date(2021, 2, 20), 'MangalJ23056160'),
    (date(2021, 2, 23), 'Surrypuria'),
    (date(2021, 2, 19), 'Preetm91')
]

# Define a test function to compare the result with the expected result
def test_q1_time(mock_environment_variables):
    file_path = "farmers-protest-tweets-2021-2-4.json"  # Replace with an appropriate file path
    try:
        # Call the q1_time function
        result = q1_time(file_path)

        # Assert the returned result matches the expected result
        assert result == expected_result

    except Exception as e:
        # Fail the test if an unexpected exception occurs
        pytest.fail(f"Test failed unexpectedly: {str(e)}")
