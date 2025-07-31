SELECT 
    b.branch_name,
    d.year,
    d.month,
    SUM(f.total) AS monthly_sales,
    ROUND(AVG(SUM(f.total)) OVER (
        PARTITION BY b.branch_name 
        ORDER BY d.year, d.month 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_3_month
FROM fact_sales f
JOIN dim_branch b ON f.branch_key = b.branch_key
JOIN dim_date d   ON f.date_key = d.date_key
GROUP BY b.branch_name, d.year, d.month
ORDER BY b.branch_name, d.year, d.month;
