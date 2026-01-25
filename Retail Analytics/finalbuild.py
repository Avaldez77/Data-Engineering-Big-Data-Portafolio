"""Build Gold marts and export Neo4j bulk-import files."""

from collections import defaultdict, Counter
from pathlib import Path
from src.utils.io import read_csv, write_csv

SILVER = Path("data/silver")
BRONZE = Path("data/bronze")
GOLD = Path("data/gold")

def main():
    GOLD.mkdir(parents=True, exist_ok=True)

    orders = read_csv(str(SILVER/"silver_orders.csv"))
    products = {p["product_id"]: p for p in read_csv(str(SILVER/"silver_products.csv"))}
    items = read_csv(str(BRONZE/"order_items.csv"))
    customers = read_csv(str(SILVER/"silver_customers.csv"))

    order_date = {o["order_id"]: o["order_date"] for o in orders}
    order_customer = {o["order_id"]: o["customer_id"] for o in orders}

    daily = defaultdict(float)
    for it in items:
        d = order_date.get(it["order_id"])
        if d:
            daily[d] += float(it["line_total"])

    gold_daily = [{"day": d, "revenue": round(v,2)} for d, v in sorted(daily.items())]
    write_csv(str(GOLD/"gold_daily_sales.csv"), ["day","revenue"], gold_daily)

    prod_rev = defaultdict(float)
    prod_qty = defaultdict(int)
    for it in items:
        pid = it["product_id"]
        prod_rev[pid] += float(it["line_total"])
        prod_qty[pid] += int(float(it["qty"]))

    top = sorted(prod_rev.items(), key=lambda x: x[1], reverse=True)[:20]
    gold_top = []
    for pid, rev in top:
        p = products.get(pid, {})
        gold_top.append({
            "product_id": pid,
            "product_name": p.get("product_name",""),
            "revenue": round(rev,2),
            "qty": prod_qty[pid]
        })
    write_csv(str(GOLD/"gold_top_products.csv"), ["product_id","product_name","revenue","qty"], gold_top)

    # Co-purchase pairs
    order_to_pids = defaultdict(list)
    for it in items:
        order_to_pids[it["order_id"]].append(it["product_id"])

    pair_strength = Counter()
    for oid, pids in order_to_pids.items():
        uniq = sorted(set(pids))
        for i in range(len(uniq)):
            for j in range(i+1, len(uniq)):
                pair_strength[(uniq[i], uniq[j])] += 1

    pairs = [{"product_id_a": a, "product_id_b": b, "shared_orders": cnt}
             for (a,b), cnt in pair_strength.most_common(200)]
    write_csv(str(GOLD/"gold_copurchase_pairs.csv"), ["product_id_a","product_id_b","shared_orders"], pairs)

    # Customer segments (rule-based)
    cust_spend = defaultdict(float)
    for it in items:
        cid = order_customer.get(it["order_id"])
        if cid:
            cust_spend[cid] += float(it["line_total"])

    seg_rows = []
    for c in customers:
        cid = c["customer_id"]
        spend = cust_spend.get(cid, 0.0)
        if spend >= 400:
            seg = "high_value"
        elif spend >= 200:
            seg = "regular"
        else:
            seg = "new_or_low"
        seg_rows.append({"customer_id": cid, "name": c["name"], "lifetime_value": round(spend,2), "segment": seg})

    write_csv(str(GOLD/"gold_customer_segments.csv"), ["customer_id","name","lifetime_value","segment"], seg_rows)

    # Neo4j exports
    neo = GOLD / "neo4j"
    neo.mkdir(parents=True, exist_ok=True)

    write_csv(str(neo/"nodes_customers.csv"),
              ["customer_id:ID(Customer)","name","segment"],
              [{"customer_id:ID(Customer)": r["customer_id"], "name": r["name"], "segment": r["segment"]} for r in seg_rows])

    write_csv(str(neo/"nodes_products.csv"),
              ["product_id:ID(Product)","name","category_id","unit_price:float"],
              [{"product_id:ID(Product)": p["product_id"], "name": p["product_name"], "category_id": p["category_id"],
                "unit_price:float": float(p["unit_price"])} for p in products.values()])

    rel_bought = read_csv(str(SILVER/"rel_bought.csv"))
    write_csv(str(neo/"rels_bought.csv"),
              [":START_ID(Customer)",":END_ID(Product)","qty_total:int","orders_count:int"],
              [{":START_ID(Customer)": r["customer_id"], ":END_ID(Product)": r["product_id"],
                "qty_total:int": int(float(r["qty_total"])), "orders_count:int": int(float(r["orders_count"]))} for r in rel_bought])

    write_csv(str(neo/"rels_copurchased.csv"),
              [":START_ID(Product)",":END_ID(Product)","shared_orders:int"],
              [{":START_ID(Product)": r["product_id_a"], ":END_ID(Product)": r["product_id_b"], "shared_orders:int": int(r["shared_orders"])} for r in pairs])

    print("✅ Gold marts built + Neo4j exports written to data/gold/neo4j/")

if __name__ == "__main__":
    main()
