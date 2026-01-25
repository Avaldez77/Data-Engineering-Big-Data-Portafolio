CREATE SCHEMA IF NOT EXISTS dataset;

CREATE OR REPLACE TABLE dataset.orders (
  order_id STRING,
  customer_id STRING,
  store_id STRING,
  city STRING,
  channel STRING,
  order_date DATE
);

CREATE OR REPLACE TABLE dataset.order_items (
  order_id STRING,
  product_id STRING,
  qty INT64,
  line_total NUMERIC
);
