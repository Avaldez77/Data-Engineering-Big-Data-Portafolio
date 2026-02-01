// Update file paths to point to data/gold/neo4j/*.csv

LOAD CSV WITH HEADERS FROM 'file:///nodes_customers.csv' AS row
MERGE (c:Customer {customer_id: row.`customer_id:ID(Customer)`})
SET c.name = row.name,
    c.segment = row.segment;

LOAD CSV WITH HEADERS FROM 'file:///nodes_products.csv' AS row
MERGE (p:Product {product_id: row.`product_id:ID(Product)`})
SET p.name = row.name,
    p.category_id = row.category_id,
    p.unit_price = toFloat(row.`unit_price:float`);

LOAD CSV WITH HEADERS FROM 'file:///rels_bought.csv' AS row
MATCH (c:Customer {customer_id: row.`:START_ID(Customer)`})
MATCH (p:Product {product_id: row.`:END_ID(Product)`})
MERGE (c)-[r:BOUGHT]->(p)
SET r.qty_total = toInteger(row.`qty_total:int`),
    r.orders_count = toInteger(row.`orders_count:int`);

LOAD CSV WITH HEADERS FROM 'file:///rels_copurchased.csv' AS row
MATCH (a:Product {product_id: row.`:START_ID(Product)`})
MATCH (b:Product {product_id: row.`:END_ID(Product)`})
MERGE (a)-[r:CO_PURCHASED_WITH]->(b)
SET r.shared_orders = toInteger(row.`shared_orders:int`);
