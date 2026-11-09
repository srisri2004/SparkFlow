"""Generate a deliberately MESSY synthetic dataset so the cleaning step has real work to do.

Usage:  python generate_data.py --orders 50000 --out data
Outputs: data/orders.csv  and  data/clickstream.log
"""
import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta

CATALOG = {
    "electronics": [("P100", 199.99), ("P101", 49.50), ("P102", 899.00), ("P103", 29.99)],
    "clothing": [("P200", 39.90), ("P201", 24.50), ("P202", 79.00), ("P203", 59.99)],
    "home": [("P300", 15.99), ("P301", 129.00), ("P302", 45.25), ("P303", 9.99)],
    "books": [("P400", 12.99), ("P401", 18.50), ("P402", 7.99), ("P403", 24.00)],
    "sports": [("P500", 89.00), ("P501", 34.99), ("P502", 149.50), ("P503", 19.99)],
}
COUNTRIES = ["USA", "UK", "India", "Germany", "Canada", "Australia"]
COUNTRY_NOISE = {"USA": ["usa", "US", "United States", " U.S.A"], "UK": ["uk", "GB", "United Kingdom"],
                 "India": ["india", "IN"], "Germany": ["germany", "DE"], "Canada": ["canada", "CA"],
                 "Australia": ["australia", "AU"]}
STATUSES = ["completed"] * 6 + ["shipped"] * 2 + ["cancelled", "returned", "pending"]


def fmt_date(d: date, rng: random.Random) -> str:
    r = rng.random()
    if r < 0.60:
        return d.strftime("%Y-%m-%d")
    if r < 0.80:
        return d.strftime("%d/%m/%Y")
    if r < 0.93:
        return d.strftime("%m-%d-%Y")
    if r < 0.98:
        return d.strftime("%Y/%m/%d")
    return rng.choice(["N/A", "not a date", "2026-13-45", ""])


def make_orders(n_orders: int, out_path: str, rng: random.Random):
    n_customers = max(50, n_orders // 8)
    cust_country = {f"C{i:05d}": rng.choice(COUNTRIES) for i in range(1, n_customers + 1)}
    customers = list(cust_country)
    start = date(2026, 1, 1)
    rows = []

    for i in range(1, n_orders + 1):
        oid = f"O{i:07d}"
        cust = rng.choice(customers)
        d = start + timedelta(days=rng.randint(0, 299))
        status = rng.choice(STATUSES)
        for _ in range(rng.choices([1, 2, 3, 4], weights=[45, 30, 17, 8])[0]):
            cat = rng.choice(list(CATALOG))
            pid, price = rng.choice(CATALOG[cat])
            qty = rng.choices([1, 2, 3, 5], weights=[60, 25, 10, 5])[0]
            country = cust_country[cust]

            # ---- inject noise ----
            c_id = cust if rng.random() > 0.02 else rng.choice(["", "NULL"])
            category = cat
            x = rng.random()
            if x < 0.03:
                category = ""
            elif x < 0.30:
                category = rng.choice([cat.upper(), cat.title(), f" {cat} "])
            price_s = f"{price:.2f}"
            x = rng.random()
            if x < 0.10:
                price_s = f"${price:,.2f}"
            elif x < 0.105:
                price_s = "abc"
            elif x < 0.115:
                price_s = "0"
            qty_s = str(qty)
            x = rng.random()
            if x < 0.015:
                qty_s = rng.choice(["0", "-1", "-3"])
            elif x < 0.020:
                qty_s = "5000"
            elif x < 0.025:
                qty_s = "two"
            country_s = country if rng.random() > 0.35 else rng.choice(COUNTRY_NOISE[country])
            status_s = status if rng.random() > 0.15 else rng.choice([status.upper(), status.title(), f" {status} "])
            if rng.random() < 0.005:
                status_s = "???"

            rows.append([oid, c_id, pid.lower() if rng.random() < 0.05 else pid, category,
                         qty_s, price_s, fmt_date(d, rng), country_s, status_s])

    # exact duplicate rows (source system exported twice)
    rows += rng.sample(rows, k=int(len(rows) * 0.03))
    rng.shuffle(rows)

    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "product_id", "category", "quantity",
                    "unit_price", "order_date", "country", "status"])
        w.writerows(rows)
    return len(rows)


def make_clickstream(n_lines: int, n_customers: int, out_path: str, rng: random.Random):
    products = [p for items in CATALOG.values() for p, _ in items]
    ts = datetime(2026, 1, 1)
    with open(out_path, "w") as f:
        for _ in range(n_lines):
            ts += timedelta(seconds=rng.randint(0, 30))
            if rng.random() < 0.03:  # malformed line
                f.write(rng.choice(["", "ERROR connection reset", "### heartbeat ###",
                                    f"{ts:%Y-%m-%d %H:%M:%S} | user=?? | action=hover"]) + "\n")
                continue
            action = rng.choices(["view", "add_to_cart", "purchase"], weights=[80, 14, 6])[0]
            user = f"c{rng.randint(1, n_customers):05d}"
            f.write(f"{ts:%Y-%m-%d %H:%M:%S} | user={user} | action={action} | product={rng.choice(products)}\n")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=20000, help="number of orders (~2.1 lines each)")
    ap.add_argument("--out", default="data")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    rng = random.Random(args.seed)
    n = make_orders(args.orders, os.path.join(args.out, "orders.csv"), rng)
    make_clickstream(args.orders * 5, max(50, args.orders // 8),
                     os.path.join(args.out, "clickstream.log"), rng)
    print(f"Wrote {n:,} order lines and {args.orders * 5:,} clickstream lines to {args.out}/")
