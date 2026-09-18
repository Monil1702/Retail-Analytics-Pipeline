import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from retail_pipeline.quality import latest_by_transaction, validation_errors


class QualityRulesTest(unittest.TestCase):
    def setUp(self):
        self.valid = {
            "transaction_id": "TX-1", "customer_id": "C-1", "product_id": "P-1",
            "transaction_ts": "2025-01-01T00:00:00Z", "updated_at": "2025-01-01T00:01:00Z",
            "quantity": "2", "unit_price": "9.99", "channel": "web",
        }

    def test_valid_record_has_no_errors(self):
        self.assertEqual(validation_errors(self.valid), [])

    def test_invalid_values_return_stable_rule_names(self):
        row = self.valid | {"quantity": 0, "unit_price": -1, "channel": "fax"}
        self.assertEqual(validation_errors(row), ["invalid_quantity", "invalid_unit_price", "invalid_channel"])

    def test_latest_record_wins_deduplication(self):
        newer = self.valid | {"quantity": 4, "updated_at": "2025-01-01T00:02:00Z"}
        result = latest_by_transaction([self.valid, newer])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["quantity"], 4)


if __name__ == "__main__":
    unittest.main()
