""" Modules for generating charts from the ETL processed data. """
import sqlite3
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Set variables
SQL_QUERIES_DIR = "./analysis/sql"
CHART_DIR = "./analysis/charts"
DW_DIR = "./data/dw/sales_dw.db"

# Create chart directory if it doesn't exist
if not os.path.exists(CHART_DIR):
    os.makedirs(CHART_DIR)

queries = {
    "monthly_sales": "monthly_sales.sql",
    "top_products": "top_products.sql",
    "customer_spending": "customer_spending.sql",
    "hourly_sales": "hourly_sales.sql"
}

# Connect to SQLite
conn = sqlite3.connect(DW_DIR)
results = {}

# Run queries and store results
for name, filename in queries.items():
    with open(os.path.join(SQL_QUERIES_DIR, filename), "r", encoding="utf-8") as file:
        sql = file.read()
        results[name] = pd.read_sql_query(sql, conn)

conn.close()

# Chart 1 : Monthly Sales Trend
plt.figure(figsize=(10, 6))
sns.lineplot(
    data=results["monthly_sales"], 
    x="month", y="monthly_sales", hue="branch_name", marker="o"
)
plt.title("Monthly Sales Trend by Branch")
plt.xlabel("Month")
plt.ylabel("Total Sales")
plt.legend(title="branch_name")
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, "monthly_sales_trend.png"))
plt.close()

# Chart 2: Top 5 Products by Sales per Branch
plt.figure(figsize=(12, 6))
sns.barplot(
    data=results["top_products"], 
    x="product_line", y="total_sales", hue="branch_name",
)
plt.title("Top 5 Products by Sales per Branch")
plt.xticks(rotation=45)
plt.ylabel("Total Sales")
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, "top_products.png"))
plt.close()

# Chart 3: Customer Spending Heatmap
pivot = results["customer_spending"].pivot(
    index="gender", columns="payment_method", values="avg_spending"
)
plt.figure(figsize=(8, 5))
sns.heatmap(pivot, annot=True, fmt=".2f", cmap="Blues")
plt.title("Average Customer Spending by Gender and Payment Type")
plt.ylabel("Gender")
plt.xlabel("Payment Type")
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, "customer_spending_heatmap.png"))
plt.close()

# Chart 4: Hourly Sales Distribution
plt.figure(figsize=(10, 6))
sns.lineplot(
    data=results["hourly_sales"], 
    x="hour", y="hourly_sales", hue="branch_name", marker="o"
)
plt.title("Hourly Sales Distribution by Branch")
plt.xlabel("Hour of Day")
plt.ylabel("Total Sales")
plt.legend(title="Branch")
plt.tight_layout()
plt.savefig(os.path.join(CHART_DIR, "hourly_sales_distribution.png"))
plt.close()

print("Charts saved in: ", CHART_DIR)
