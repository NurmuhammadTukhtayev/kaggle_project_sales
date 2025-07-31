""" Module for loading data into the sales data warehouse. """
import sqlite3
import os
import pandas as pd

# Variables
FILE_DIR = './data/processed'
SQL_DIR = './data/dw'

def load_data_to_dw():
    """Load the processed data into the sales data warehouse."""
    try:
        # Create output directory if it doesn't exist
        if not os.path.exists(SQL_DIR):
            os.makedirs(SQL_DIR)

        # Connect to SQLite (creates a new file if not exists)
        conn = sqlite3.connect(f"{SQL_DIR}/sales_dw.db")
        cursor = conn.cursor()

        # Read the CSV files
        dim_branch = pd.read_csv(f"{FILE_DIR}/dim_branch.csv")
        dim_customer = pd.read_csv(f"{FILE_DIR}/dim_customer.csv")
        dim_product = pd.read_csv(f"{FILE_DIR}/dim_product.csv")
        dim_payment = pd.read_csv(f"{FILE_DIR}/dim_payment.csv")
        dim_date = pd.read_csv(f"{FILE_DIR}/dim_date.csv")
        dim_time = pd.read_csv(f"{FILE_DIR}/dim_time.csv")
        fact_sales = pd.read_csv(f"{FILE_DIR}/fact_sales.csv")

        # Write each DataFrame to SQLite
        dim_branch.to_sql("dim_branch", conn, if_exists="replace", index=False)
        dim_customer.to_sql("dim_customer", conn, if_exists="replace", index=False)
        dim_product.to_sql("dim_product", conn, if_exists="replace", index=False)
        dim_payment.to_sql("dim_payment", conn, if_exists="replace", index=False)
        dim_date.to_sql("dim_date", conn, if_exists="replace", index=False)
        dim_time.to_sql("dim_time", conn, if_exists="replace", index=False)
        fact_sales.to_sql("fact_sales", conn, if_exists="replace", index=False)

        # Create indexes for performance
        cursor.executescript("""
        PRAGMA foreign_keys = ON;

        -- Example: Add primary keys
        CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_branch ON dim_branch(branch_key);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_customer ON dim_customer(customer_key);

        -- Example: Fact to Dimension FK
        CREATE INDEX IF NOT EXISTS idx_fact_branch ON fact_sales(branch_key);
        CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_sales(customer_key);
        """)

        conn.commit()
        conn.close()

        return 0
    except FileNotFoundError as fnf_error:
        print("File not found during loading. Error message:", fnf_error)
        return 1
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        return 1
