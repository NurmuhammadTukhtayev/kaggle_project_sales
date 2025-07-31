""" Utility functions for ETL processes, including date and time dimension generation. """
import itertools
import pandas as pd

def generate_date_dimensions(start_date: str, end_date: str):
    """ Generate date and time dimensions for ETL processes."""
    try:
        # Date dimension
        dates = pd.date_range(start=start_date, end=end_date)

        # Create DimDate table
        dim_date = pd.DataFrame({"date": dates})
        dim_date["date_key"]      = dim_date["date"].dt.strftime("%Y%m%d").astype(int)
        dim_date["date"]        = dim_date["date"]
        dim_date["year"]        = dim_date["date"].dt.year
        dim_date["quarter"]     = dim_date["date"].dt.quarter
        dim_date["month"]       = dim_date["date"].dt.month
        dim_date["month_name"]   = dim_date["date"].dt.month_name()
        dim_date["week"]        = dim_date["date"].dt.isocalendar().week
        dim_date["day"]         = dim_date["date"].dt.day
        dim_date["day_name"]     = dim_date["date"].dt.day_name()
        dim_date["is_weekend"]   = dim_date["day_name"].isin(["Saturday", "Sunday"])

        return dim_date
    except ValueError as e:
        print(f"Error generating date dimensions: {e}")
        return pd.DataFrame()

def generate_time_dimensions() -> pd.DataFrame:
    """ Generate time dimensions for ETL processes."""
    try:
        hours = range(0, 24)
        minutes = range(0, 60)
        time_list = [f"{h:02d}:{m:02d}:00" for h, m in itertools.product(hours, minutes)]

        # Generate DimTime table
        dim_time = pd.DataFrame({"time": pd.to_datetime(time_list, format="%H:%M:%S").time})
        dim_time["time_key"]   = dim_time.index + 1
        dim_time["hour"]     = [t.hour for t in dim_time["time"]]
        dim_time["minute"]   = [t.minute for t in dim_time["time"]]
        dim_time["AM_PM"]    = ["AM" if t.hour < 12 else "PM" for t in dim_time["time"]]

        return dim_time
    except ValueError as e:
        print(f"Error generating time dimensions: {e}")
        return pd.DataFrame()
