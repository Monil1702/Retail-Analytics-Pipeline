#!/usr/bin/env python3
"""Generate deterministic retail transactions for local scale testing."""

import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=500_000)
    parser.add_argument("--output", type=Path, default=Path("data/generated/transactions.csv"))
    args = parser.parse_args()
    rng = random.Random(2025)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fields = ["transaction_id", "customer_id", "product_id", "transaction_ts", "quantity", "unit_price", "channel", "region", "updated_at"]
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for index in range(args.rows):
            event_time = start + timedelta(seconds=index * 11)
            writer.writerow({
                "transaction_id": f"TX-{index + 1:09d}",
                "customer_id": f"C-{rng.randint(1, 50000):06d}",
                "product_id": f"P-{rng.randint(1, 5000):05d}",
                "transaction_ts": event_time.isoformat(),
                "quantity": rng.randint(1, 8),
                "unit_price": f"{rng.uniform(1.0, 750.0):.2f}",
                "channel": rng.choice(["web", "store", "mobile", "marketplace"]),
                "region": rng.choice(["Quebec", "Ontario", "West", "Atlantic"]),
                "updated_at": (event_time + timedelta(minutes=5)).isoformat(),
            })


if __name__ == "__main__":
    main()
