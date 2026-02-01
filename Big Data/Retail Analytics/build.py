"""Build Silver layer: cleaned entities + relationship tables."""

from collections import defaultdict
from pathlib import Path
from src.utils.io import read_csv, write_csv

BRONZE = Path("data/bronze")
SILVER = Path("data/silver")

def main():
    SILVER.mkdir(parents=True, exist_ok=True)

    customers = read_csv(str(BRONZE/"customers.csv"))
    products = read_csv(str(BRONZE/"products.csv"))
    orders = read_csv(str(BRONZE/"orders.csv"))
    items = read_csv(str(BRONZE/"order_items.csv"))

    order_to_customer = {o["order_id"]: o["customer_id"] for o in orders}
    cp_qty = defaultdict(int)
    cp_orders = defaultdict(int)

    for it in items:
        oid = it["order_id"]
        cid = order_to_customer.get(oid)
        pid = it["product_id"]
        qty = int(float(it["qty"]))
        if cid and pid:
            cp_qty[(cid,pid)] += qty
            cp_orders[(cid,pid)] += 1

    rel_bought = []
    for (cid,pid), qty in cp_qty.items():
        rel_bought.append({
            "customer_id": cid,
            "product_id": pid,
            "qty_total": qty,
            "orders_count": cp_orders[(cid,pid)],
        })

    write_csv(str(SILVER/"silver_customers.csv"), ["customer_id","name"], customers)
    write_csv(str(SILVER/"silver_products.csv"), ["product_id","product_name","category_id","unit_price"], products)
    write_csv(str(SILVER/"silver_orders.csv"), ["order_id","customer_id","store_id","city","channel","order_date"], orders)
    write_csv(str(SILVER/"rel_bought.csv"), ["customer_id","product_id","qty_total","orders_count"], rel_bought)

    rel_ordered_at = [{"order_id": o["order_id"], "store_id": o["store_id"]} for o in orders]
    write_csv(str(SILVER/"rel_ordered_at.csv"), ["order_id","store_id"], rel_ordered_at)

    print("✅ Silver built.")

if __name__ == "__main__":
    main()
