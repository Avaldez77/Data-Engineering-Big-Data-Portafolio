CREATE OR REPLACE TABLE dataset.daily_revenue AS
SELECT order_date, SUM(line_total) AS revenue
FROM dataset.orders o
JOIN dataset.order_items oi USING(order_id)
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE TABLE dataset.top_products AS
SELECT product_id, SUM(qty) AS qty, SUM(line_total) AS revenue
FROM dataset.order_items
GROUP BY 1
ORDER BY revenue DESC
LIMIT 20;
