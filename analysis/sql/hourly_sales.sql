SELECT 
    b.branch_name,
    t.hour,
    SUM(f.total) AS hourly_sales,
    ROUND(
        100.0 * SUM(f.total) / SUM(SUM(f.total)) OVER (PARTITION BY b.branch_name), 
        2
    ) AS percentage_of_total_sales
FROM fact_sales f
JOIN dim_branch b ON f.branch_key = b.branch_key
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY b.branch_name, t.hour
ORDER BY b.branch_name, t.hour;
