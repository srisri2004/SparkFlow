from pipeline.cleaning import clean_orders
from pipeline.schemas import RAW_ORDERS_SCHEMA
from pipeline.rdd_jobs import parse_line

ROWS = [
    # order_id, customer, product, category, qty, price, date, country, status
    ("O1", "c1", "p1", "books", "2", "$1,200.50", "05/03/2026", "us", " Completed "),  # good (messy)
    ("O1", "c1", "p1", "books", "2", "$1,200.50", "05/03/2026", "us", " Completed "),  # duplicate
    ("O2", "",   "p2", "home",  "1", "10.00",     "2026-03-01", "UK", "shipped"),     # missing customer
    ("O3", "c3", "p3", "",      "-2", "10.00",    "2026-03-01", "India", "shipped"),  # bad qty
    ("O4", "c4", "p4", "sports", "1", "abc",      "2026-03-01", "India", "shipped"),  # bad price
    ("O5", "c5", "p5", "sports", "1", "5",        "N/A",        "India", "shipped"),  # bad date
    ("O6", "c6", "p6", None,     "3", "5",        "03-25-2026", "Germany", "pending"),  # good, imputed category
]


def test_clean_and_reject(spark):
    raw = spark.createDataFrame(ROWS, RAW_ORDERS_SCHEMA)
    clean, rejected = clean_orders(spark, raw)

    good = {r["order_id"]: r for r in clean.collect()}
    assert set(good) == {"O1", "O6"}               # duplicates collapsed, bad rows removed
    assert good["O1"]["unit_price"] == 1200.50     # "$1,200.50" parsed
    assert good["O1"]["country"] == "USA"          # "us" standardised
    assert good["O1"]["status"] == "completed"     # trimmed + lower-cased
    assert str(good["O1"]["order_date"]) == "2026-03-05"
    assert good["O6"]["category"] == "Unknown"     # imputed
    assert str(good["O6"]["order_date"]) == "2026-03-25"
    assert good["O6"]["line_total"] == 15.0

    reasons = {r["order_id"]: r["reject_reason"] for r in rejected.collect()}
    assert reasons == {
        "O2": "missing_customer_id",
        "O3": "bad_quantity",
        "O4": "bad_price",
        "O5": "bad_date",
    }


def test_parse_line():
    ok = "2026-01-01 10:00:00 | user=c00001 | action=view | product=P100"
    assert parse_line(ok) == ("2026-01-01", "C00001", "view", "P100")
    assert parse_line("ERROR connection reset") is None
    assert parse_line("2026-01-01 10:00:00 | user=?? | action=hover") is None
