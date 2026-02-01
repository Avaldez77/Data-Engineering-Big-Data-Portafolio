CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE;
CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE;

CREATE INDEX product_category IF NOT EXISTS FOR (p:Product) ON (p.category_id);
CREATE INDEX customer_segment IF NOT EXISTS FOR (c:Customer) ON (c.segment);
