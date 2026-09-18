"""Dependency-free quality rules shared by tests and pipeline documentation."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Mapping

ALLOWED_CHANNELS = frozenset({"web", "store", "mobile", "marketplace"})
REQUIRED_FIELDS = ("transaction_id", "customer_id", "product_id", "transaction_ts", "updated_at")


def validation_errors(row: Mapping[str, object]) -> list[str]:
    """Return stable rule identifiers for an input record."""
    errors: list[str] = []
    for field in REQUIRED_FIELDS:
        if row.get(field) in (None, ""):
            errors.append(f"missing_{field}")

    try:
        if int(str(row.get("quantity", ""))) <= 0:
            errors.append("invalid_quantity")
    except (TypeError, ValueError):
        errors.append("invalid_quantity")

    try:
        if Decimal(str(row.get("unit_price", ""))) < 0:
            errors.append("invalid_unit_price")
    except (InvalidOperation, TypeError, ValueError):
        errors.append("invalid_unit_price")

    if row.get("channel") not in ALLOWED_CHANNELS:
        errors.append("invalid_channel")
    return errors


def latest_by_transaction(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Deduplicate records, retaining the most recently updated transaction."""
    latest: dict[object, dict[str, object]] = {}
    for row in rows:
        key = row.get("transaction_id")
        updated = datetime.fromisoformat(str(row["updated_at"]).replace("Z", "+00:00"))
        previous = latest.get(key)
        if previous is None:
            latest[key] = row
            continue
        previous_updated = datetime.fromisoformat(str(previous["updated_at"]).replace("Z", "+00:00"))
        if updated > previous_updated:
            latest[key] = row
    return list(latest.values())
