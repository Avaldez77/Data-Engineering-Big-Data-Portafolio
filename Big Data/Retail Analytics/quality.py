"""Run lightweight data-quality checks on Silver."""

from pathlib import Path
from src.utils.io import read_csv
from src.utils.dq import require_non_empty, require_unique

SILVER = Path("data/silver")

def main():
    customers = read_csv(str(SILVER/"silver_customers.csv"))
    products = read_csv(str(SILVER/"silver_products.csv"))
    orders = read_csv(str(SILVER/"silver_orders.csv"))

    checks = []
    checks.append(require_non_empty(customers, "customer_id"))
    checks.append(require_unique(customers, "customer_id"))
    checks.append(require_non_empty(products, "product_id"))
    checks.append(require_unique(products, "product_id"))
    checks.append(require_non_empty(orders, "order_id"))
    checks.append(require_unique(orders, "order_id"))

    print("DQ SUMMARY")
    ok_all = True
    for ok, msg in checks:
        print(("PASS" if ok else "FAIL") + " - " + msg)
        ok_all = ok_all and ok

    if not ok_all:
        raise SystemExit("❌ Data quality failed.")
    print("✅ Data quality passed.")

if __name__ == "__main__":
    main()
