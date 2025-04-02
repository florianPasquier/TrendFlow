WITH sales AS (
    SELECT * FROM `my_project.raw_data.sales`
),
trends AS (
    SELECT * FROM `my_project.raw_data.trends`
),
products AS (
    SELECT * FROM `my_project.raw_data.products`
)

SELECT s.*, t.trend_score, p.category
FROM sales s
LEFT JOIN trends t ON s.product_id = t.product_id
LEFT JOIN products p ON s.product_id = p.product_id;
