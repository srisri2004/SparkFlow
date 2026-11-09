"""RDD jobs: the low-level API is the right tool for free-form text logs and
custom per-record logic (combinatorics) that don't fit a tabular schema."""
import re
from itertools import combinations
from operator import add
from typing import Dict, List, Tuple

from pyspark import SparkContext
from pyspark.sql import DataFrame

LOG_PATTERN = re.compile(
    r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}) \| user=(\w+) \| "
    r"action=(view|add_to_cart|purchase) \| product=(\w+)$"
)


def parse_line(line: str):
    """Parse one clickstream line -> (date, user, action, product) or None."""
    m = LOG_PATTERN.match(line.strip())
    if not m:
        return None
    date, _time, user, action, product = m.groups()
    return (date, user.upper(), action, product.upper())


def _to_funnel(item: Tuple[str, Dict[str, int]]):
    product, d = item
    views, carts, buys = d.get("view", 0), d.get("add_to_cart", 0), d.get("purchase", 0)
    conv = round(buys / views * 100, 2) if views else None
    return (product, views, carts, buys, conv)


def clickstream_funnel(sc: SparkContext, path: str) -> Dict:
    """Parse raw log text with RDDs, drop malformed lines, build a product funnel."""
    bad_lines = sc.accumulator(0)  # counts malformed lines across all executors

    def parse_and_count(line: str):
        rec = parse_line(line)
        if rec is None:
            bad_lines.add(1)
        return rec

    parsed = sc.textFile(path).map(parse_and_count).filter(lambda r: r is not None).cache()

    action_counts = dict(parsed.map(lambda r: (r[2], 1)).reduceByKey(add).collect())

    funnel = (
        parsed.map(lambda r: ((r[3], r[2]), 1))
        .reduceByKey(add)                                  # (product, action) -> n
        .map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))     # product -> (action, n)
        .groupByKey()
        .mapValues(dict)
        .map(_to_funnel)
    )
    # takeOrdered avoids a full sort; products with no views sort last.
    top_funnel = funnel.takeOrdered(
        10, key=lambda t: -(t[4] if t[4] is not None else -1)
    )

    active_users = parsed.map(lambda r: r[1]).distinct().count()
    valid_lines = parsed.count()

    result = {
        "valid_lines": valid_lines,
        "malformed_lines": bad_lines.value,  # read AFTER actions have run
        "active_users": active_users,
        "action_counts": action_counts,
        "top_products_by_view_to_purchase_pct": [
            {"product": p, "views": v, "add_to_cart": c, "purchases": b, "conversion_pct": cv}
            for p, v, c, b, cv in top_funnel
        ],
    }
    parsed.unpersist()
    return result


def copurchase_pairs(clean: DataFrame, top_n: int = 10) -> List[Dict]:
    """Market-basket analysis: which product pairs are bought in the same order?"""
    baskets = (
        clean.select("order_id", "product_id")
        .rdd.map(lambda row: (row["order_id"], row["product_id"]))
        .distinct()
        .groupByKey()
        .mapValues(lambda items: sorted(set(items)))
        .filter(lambda kv: len(kv[1]) > 1)
    )
    pairs = baskets.flatMap(
        lambda kv: [(pair, 1) for pair in combinations(kv[1], 2)]
    ).reduceByKey(add)
    top = pairs.takeOrdered(top_n, key=lambda kv: (-kv[1], kv[0]))
    return [{"product_a": a, "product_b": b, "orders_together": n} for (a, b), n in top]
