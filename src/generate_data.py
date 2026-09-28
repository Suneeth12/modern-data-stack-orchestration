"""Generate raw source tables — deliberately a bit dirty.

Real source data has nulls, duplicates, and orphan foreign keys. We inject some
so the data-quality tests downstream have something to catch (and so the staging
models have real cleaning to do).
"""
import csv
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def main():
    DATA.mkdir(exist_ok=True)
    rng = np.random.default_rng(0)

    # raw_customers: a few null regions, one duplicate id
    with (DATA / "raw_customers.csv").open("w", newline="") as f:
        w = csv.writer(f); w.writerow(["customer_id", "region", "signup_year"])
        for cid in range(1, 201):
            region = rng.choice(["North", "South", "East", "West", ""], p=[.24, .24, .24, .24, .04])
            w.writerow([cid, region, int(rng.integers(2019, 2026))])
        w.writerow([200, "North", 2024])              # duplicate customer_id

    # raw_orders: some null amounts, a couple of orphan customer_ids
    with (DATA / "raw_orders.csv").open("w", newline="") as f:
        w = csv.writer(f); w.writerow(["order_id", "customer_id", "amount", "status"])
        for oid in range(1, 2001):
            cid = int(rng.integers(1, 210))           # 201-209 are orphans
            amt = "" if rng.random() < 0.03 else round(float(rng.gamma(2, 40)), 2)
            status = rng.choice(["paid", "refunded", "pending"], p=[.8, .1, .1])
            w.writerow([oid, cid, amt, status])

    print("Wrote data/raw_customers.csv (200 + 1 dup) and data/raw_orders.csv (2000)")


if __name__ == "__main__":
    main()
