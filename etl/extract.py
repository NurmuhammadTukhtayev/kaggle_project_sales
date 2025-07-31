"""Modules for shell util and kaggle."""
import os
import shutil
import kagglehub

# Final path for dataset
DESTINATION_DIRECTORY = "./data/raw"
FILE_NAME = "supermarket_sales.csv"

def extract_data():
    """Extracts the supermarket sales dataset from Kaggle and saves it to the raw data directory."""
    try:
        # create path if not exists
        if not os.path.isdir(DESTINATION_DIRECTORY):
            os.makedirs(DESTINATION_DIRECTORY)

        # Download latest version
        source_file = kagglehub.dataset_download("lovishbansal123/sales-of-a-supermarket")

        print("Path to dataset files:", source_file)

        # Copy dataset into raw
        shutil.copy(f"{source_file}/supermarket_sales.csv", f"{DESTINATION_DIRECTORY}/{FILE_NAME}")
        print(f"'{source_file}' copied to '{DESTINATION_DIRECTORY}/{FILE_NAME}'")

        return 0
    except FileNotFoundError as fnf_error:
        print("File not found during extraction. Error message:", fnf_error)
        return 1
    except PermissionError as perm_error:
        print("Permission error during extraction. Error message:", perm_error)
        return 1
    except OSError as os_error:
        print("OS error during extraction. Error message:", os_error)
        return 1
