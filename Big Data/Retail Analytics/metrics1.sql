CREATE OR REPLACE VIEW retail.daily_revenue AS
SELECT order_date, SUM(oi.line_total) AS revenue
FROM retail.orders o
JOIN retail.order_items oi USING(order_id)
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW retail.top_products AS
SELECT
  oi.product_id,
  SUM(oi.qty) AS qty,
  SUM(oi.line_total) AS revenue
FROM retail.order_items oi
GROUP BY 1
ORDER BY revenue DESC
LIMIT 20;
