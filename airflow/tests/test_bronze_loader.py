"""Offline tests: run via python -m unittest discover -s airflow/tests -p 'test_*.py'."""
import csv
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from icommerce_ingestion.datasets import DATASETS
from icommerce_ingestion.generic_batches import digest_file, prepare_batch
from icommerce_ingestion.bronze_loader import (
    BASELINE, BASELINE_COUNTS, build_bronze_file, build_copy_sql,
    classify_mode, cleanup_prepared, table_for, validate_mode, verify_bronze_file,
)


class FinalBronzeTests(unittest.TestCase):
    def test_baseline_all_seven(self):
        self.assertEqual(set(BASELINE), set(DATASETS))
        self.assertEqual(set(BASELINE_COUNTS), set(DATASETS))
        for dataset, digest in BASELINE.items():
            self.assertEqual(classify_mode(dataset, digest), "initial")
            with self.assertRaises(ValueError):
                validate_mode(dataset, digest, "incremental")

    def test_copy_into_original_schema_only(self):
        for dataset in DATASETS:
            sql = build_copy_sql(dataset, BASELINE[dataset])
            self.assertIn(f"COPY INTO ICOMMERCE.BRONZE.{dataset} (", sql)
            self.assertIn("FROM (SELECT ", sql)
            self.assertIn("FROM @ICOMMERCE.BRONZE.ICOMMERCE_CSV_STAGE s)", sql)
            self.assertIn("INGESTED_AT", sql)
            self.assertNotIn("_DEV", sql)
            self.assertEqual(table_for(dataset), f"ICOMMERCE.BRONZE.{dataset}")

    def create(self, directory, dataset, rows):
        path = directory / f"{dataset}.csv"
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.writer(stream, delimiter="|")
            writer.writerow(DATASETS[dataset].columns)
            writer.writerows(rows)
        return path

    def test_incremental_customer_timestamp_and_cleanup(self):
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            file = self.create(folder, "CUSTOMER", [
                ["A001", "Smith", "Ann", "", "75001", "Paris", "France", "T001"],
                ["A002", "Jones", "Bob", "", "75002", "Paris", "France", "T001"],
            ])
            info = prepare_batch(file, folder, "CUSTOMER", digest_file(file))
            result = build_bronze_file(info, "incremental")
            verify_bronze_file(result)
            with Path(result["bronze_path"]).open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream, delimiter="|"))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["ingested_at"], rows[1]["ingested_at"])
            self.assertTrue(rows[0]["ingested_at"])
            cleanup_prepared(result)
            self.assertFalse(Path(result["bronze_path"]).exists())
            self.assertFalse(Path(result["load_path"]).exists())
            self.assertTrue(file.exists())

    def test_duplicate_key_incremental_is_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            file = self.create(folder, "PRODUCT", [
                ["P001", "One", "1.0", "SC001"], ["P001", "Two", "2.0", "SC001"],
            ])
            info = prepare_batch(file, folder, "PRODUCT", digest_file(file))
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                build_bronze_file(info, "incremental")

    def test_demo_key_incremental_is_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            file = self.create(folder, "PRODUCT", [["DEMO-P001", "Foo", "1", "SC001"]])
            info = prepare_batch(file, folder, "PRODUCT", digest_file(file))
            with self.assertRaisesRegex(ValueError, "DEMO"):
                build_bronze_file(info, "incremental")

    def test_unexpected_mode_rejected(self):
        with self.assertRaises(ValueError):
            classify_mode("UNKNOWN", "0" * 64)
        with self.assertRaises(ValueError):
            validate_mode("CUSTOMER", "0" * 64, "initial")


if __name__ == "__main__":
    unittest.main()
