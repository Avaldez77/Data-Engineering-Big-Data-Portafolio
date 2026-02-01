CREATE SCHEMA IF NOT EXISTS retail;

CREATE TABLE IF NOT EXISTS retail.orders (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT,
  store_id TEXT,
  city TEXT,
  channel TEXT,
  order_date DATE
);

CREATE TABLE IF NOT EXISTS retail.order_items (
  order_id TEXT,
  product_id TEXT,
  qty INT,
  line_total NUMERIC
);
