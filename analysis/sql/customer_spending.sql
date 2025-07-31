SELECT 
    c.gender,
    pay.payment_method,
    ROUND(AVG(f.total), 2) AS avg_spending
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_payment pay ON f.payment_key = pay.payment_key
GROUP BY c.gender, pay.payment_method
ORDER BY avg_spending DESC;
