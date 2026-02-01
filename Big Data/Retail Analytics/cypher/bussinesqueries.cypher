// Customer purchase profile
MATCH (c:Customer {customer_id: 'C001'})-[r:BOUGHT]->(p:Product)
RETURN p.name, r.orders_count, r.qty_total
ORDER BY r.orders_count DESC, r.qty_total DESC
LIMIT 10;

// Customers with similar baskets (shared products)
MATCH (c:Customer {customer_id:'C001'})-[:BOUGHT]->(p:Product)<-[:BOUGHT]-(other:Customer)
WHERE other.customer_id <> c.customer_id
RETURN other.customer_id, other.name, count(DISTINCT p) AS shared_products
ORDER BY shared_products DESC
LIMIT 10;

// Related products (co-purchased network)
MATCH (:Product {product_id:'P100'})-[r:CO_PURCHASED_WITH]->(p2:Product)
RETURN p2.product_id, p2.name, r.shared_orders
ORDER BY r.shared_orders DESC
LIMIT 10;

// Segment counts
MATCH (c:Customer)
RETURN c.segment, count(*) AS customers
ORDER BY customers DESC;
