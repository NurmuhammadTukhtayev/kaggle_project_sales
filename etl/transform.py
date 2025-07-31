""" Modules for transforming data in the ETL process. """
import pandas as pd
from utils import generate_date_dimensions, generate_time_dimensions

# Variables
START_DATE = '2019-01-01'
END_DATE = '2021-12-31'

# Load the raw data
df = pd.read_csv('./data/raw/supermarket_sales.csv')

def rename_columns(input_df):
    """Rename columns for consistency"""
    renamed_df = input_df.rename(columns={
        "Invoice ID": "invoice_id",
        "Branch": "branch_name",
        "City": "city",
        "Customer type": "customer_type",
        "Gender": "gender",
        "Product line": "product_line",
        "Unit price": "unit_price",
        "Quantity": "quantity",
        "Tax 5%": "tax_5_percent",
        "Total": "total",
        "Date": "date",
        "Time": "time",
        "Payment": "payment_method",
        "cogs": "cost_of_goods_sold",
        "gross margin percentage": "gross_margin_percentage",
        "gross income": "gross_income",
        "Rating": "rating"
    })

    # Parse date and time columns
    renamed_df["date"] = pd.to_datetime(renamed_df["date"], format="%m/%d/%Y")
    renamed_df["date_key"] = renamed_df["date"].dt.strftime("%Y%m%d").astype(int)

    renamed_df["time"] = pd.to_datetime(renamed_df["time"], format="%H:%M").dt.time

    return renamed_df

def extract_branch_dimensions(input_df):
    """Branch dimension extraction"""
    dim_branch = (
        input_df[["branch_name", "city"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .reset_index()
        .rename(columns={"index": "branch_key"})
    )
    dim_branch["branch_key"] = dim_branch["branch_key"] + 1 # Start keys from 1

    return dim_branch

def extract_customer_dimensions(input_df):
    """ Customer dimension extraction """
    dim_customer = (
        input_df[["customer_type", "gender"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .reset_index()
        .rename(columns={"index": "customer_key"})
    )
    dim_customer["customer_key"] = dim_customer["customer_key"] + 1 # Start keys from 1

    return dim_customer

def extract_product_dimensions(input_df):
    """ Product dimension extraction """
    dim_product = (
        input_df[["product_line"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .reset_index()
        .rename(columns={"index": "product_key"})
    )
    dim_product["product_key"] = dim_product["product_key"] + 1 # Start keys from 1

    return dim_product

def extract_payment_dimensions(input_df):
    """ Payment dimension extraction """
    dim_payment = (
        input_df[["payment_method"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .reset_index()
        .rename(columns={"index": "payment_key"})
    )
    dim_payment["payment_key"] = dim_payment["payment_key"] + 1 # Start keys from 1

    return dim_payment

def extract_date_dimensions():
    """ Date dimension extraction """
    dim_date = generate_date_dimensions(START_DATE, END_DATE)

    return dim_date

def extract_time_dimensions():
    """ Time dimension extraction """
    dim_time = generate_time_dimensions()

    return dim_time

def extract_fact_sales(sales_df, dim_branch, dim_customer, dim_product, dim_payment, dim_date, dim_time):
    """ Fact table extraction """
    fact_sales = (
        sales_df
        # merge branch
        .merge(dim_branch,   on=["branch_name","city"], how="left")
        # merge customer
        .merge(dim_customer, on=["customer_type","gender"], how="left")
        # merge product
        .merge(dim_product,  on=["product_line"], how="left")
        # merge payment
        .merge(dim_payment,  on=["payment_method"], how="left")
        # merge date
        .merge(dim_date,     on=["date_key"], how="left")
        # # merge time
        .merge(dim_time,     on=["time"], how="left")
    )


    # Select only the keys + measures for the final fact table
    fact_sales = fact_sales[[
        "invoice_id",
        "date_key", "time_key", 
        # "date_key", "time",
        "branch_key", "customer_key", "product_key", "payment_key",
        "unit_price", "quantity", "tax_5_percent", "total", "cost_of_goods_sold",
        "gross_margin_percentage", "gross_income", "rating"
    ]]

    return fact_sales
