"""This script is the entry point for the ETL process."""
import os
import logging
import pandas as pd
from extract import extract_data
from transform import rename_columns, extract_branch_dimensions, extract_customer_dimensions, extract_product_dimensions, extract_payment_dimensions, extract_date_dimensions, extract_time_dimensions, extract_fact_sales
from load import load_data_to_dw

def setup_logging():
    """Sets up logging for the ETL process."""
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create a log directory if it doesn't exist
    if not os.path.exists('./logs'):
        os.makedirs('./logs')

    # File handler
    file_handler = logging.FileHandler('./logs/etl.log')
    file_handler.setFormatter(log_formatter)
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    logger.addHandler(console_handler)

if __name__ == "__main__":
    setup_logging()
    try:
        OUTPUT_DIR = './data/processed'
        logging.info("Starting ETL process.")

        # PHASE 1: Data Extraction
        logging.info("PHASE 1: Data Extraction started.")
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            logging.info("Created output directory: %s", OUTPUT_DIR)

        RETURN_CODE = extract_data()
        logging.info("Data extraction function executed.")

        if RETURN_CODE != 0:
            logging.error("Data extraction failed with return code.")
            raise RuntimeError("Data extraction failed with return code")
        logging.info("Data extraction completed successfully.")

        # Load the data into a DataFrame
        df = pd.read_csv('./data/raw/supermarket_sales.csv')
        logging.info("Loaded raw data into DataFrame.")

        # PHASE 2: Data Transformation
        logging.info("PHASE 2: Data Transformation started.")
        df = rename_columns(df)
        logging.info("Renamed columns in DataFrame.")

        dim_branch = extract_branch_dimensions(df)
        logging.info("Extracted branch dimensions.")
        dim_customer = extract_customer_dimensions(df)
        logging.info("Extracted customer dimensions.")
        dim_product = extract_product_dimensions(df)
        logging.info("Extracted product dimensions.")
        dim_payment = extract_payment_dimensions(df)
        logging.info("Extracted payment dimensions.")
        dim_date = extract_date_dimensions()
        logging.info("Extracted date dimensions.")
        dim_time = extract_time_dimensions()
        logging.info("Extracted time dimensions.")

        fact_sales = extract_fact_sales(df, dim_branch, dim_customer, dim_product, dim_payment, dim_date, dim_time)
        logging.info("Extracted fact sales.")

        dim_branch.to_csv(f"{OUTPUT_DIR}/dim_branch.csv", index=False)
        logging.info("Saved dim_branch.csv.")
        dim_customer.to_csv(f"{OUTPUT_DIR}/dim_customer.csv", index=False)
        logging.info("Saved dim_customer.csv.")
        dim_product.to_csv(f"{OUTPUT_DIR}/dim_product.csv", index=False)
        logging.info("Saved dim_product.csv.")
        dim_payment.to_csv(f"{OUTPUT_DIR}/dim_payment.csv", index=False)
        logging.info("Saved dim_payment.csv.")
        dim_date.to_csv(f"{OUTPUT_DIR}/dim_date.csv", index=False)
        logging.info("Saved dim_date.csv.")
        dim_time.to_csv(f"{OUTPUT_DIR}/dim_time.csv", index=False)
        logging.info("Saved dim_time.csv.")
        fact_sales.to_csv(f"{OUTPUT_DIR}/fact_sales.csv", index=False)
        logging.info("Saved fact_sales.csv.")

        logging.info("Data transformation completed successfully.")

        # PHASE 3: Data Loading
        logging.info("PHASE 3: Data Loading started.")
        LOAD_STATUS = load_data_to_dw()
        logging.info("Data loading function executed.")
        if LOAD_STATUS != 0:
            logging.error("Data loading failed with return code.")
            raise RuntimeError("Data loading failed with return code")
        
        logging.info("Data loading completed successfully.")
        logging.info("ETL process finished.")

    except RuntimeError as e:
        logging.error("An error occurred during ETL process: %s", e)
