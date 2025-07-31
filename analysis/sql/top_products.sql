SELECT *
FROM (
    SELECT 
        b.branch_name,
        p.product_line,
        SUM(f.total) AS total_sales,
        RANK() OVER (
            PARTITION BY b.branch_name 
            ORDER BY SUM(f.total) DESC
        ) AS product_rank
    FROM fact_sales f
    JOIN dim_branch b ON f.branch_key = b.branch_key
    JOIN dim_product p ON f.product_key = p.product_key
    GROUP BY b.branch_name, p.product_line
) ranked
WHERE product_rank <= 5
ORDER BY branch_name, product_rank;
