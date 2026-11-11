"""Command to run this unit test: python -m unittest discover -s airflow/tests -p test_generic_batches.py -v"""
import csv
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from icommerce_ingestion.datasets import DATASETS, dataset_from_filename  # noqa: E402
from icommerce_ingestion.generic_batches import (  # noqa: E402
    digest_file, prepare_batch, verify_batch,
)


class GenericBatchTests(unittest.TestCase):
    def run_case(self, dataset: str, text: str, expected: int) -> list[dict]:
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "source.csv"
            source.write_text(text, encoding="utf-8")
            info = prepare_batch(source, Path(temp), dataset, digest_file(source))
            verify_batch(info)
            self.assertEqual(info["row_count"], expected)
            with Path(info["load_path"]).open(encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle, delimiter="|"))
            self.assertEqual(len(rows), expected)
            self.assertEqual(rows[0]["source_sha256"], info["batch_id"])
            return rows

    def test_dataset_registry_all_seven(self):
        self.assertEqual(len(DATASETS), 7)
        self.assertEqual(dataset_from_filename("CATEGORY_batch_001.csv"), "CATEGORY")
        self.assertEqual(dataset_from_filename("sub_category_002.csv"), "SUB_CATEGORY")
        self.assertEqual(dataset_from_filename("TYPE_CUSTOMER.csv"), "TYPE_CUSTOMER")
        self.assertEqual(dataset_from_filename("detail_sale_nov.csv"), "DETAIL_SALE")
        for filename in ("OTHER.csv", "CATEGORY.txt", "SALE2.csv"):
            with self.assertRaises(ValueError):
                dataset_from_filename(filename)

    def test_category_multiline_and_duplicates(self):
        rows = self.run_case("CATEGORY", "cd_category|lib_category\nC001|Technology\nC004|Other\n categories\nC001|TECHNOLOGY\n", 3)
        self.assertEqual(rows[1]["lib_category"], "Other categories")
        self.assertEqual(sum(x["cd_category"] == "C001" for x in rows), 2)

    def test_subcategory_multiline(self):
        rows = self.run_case("SUB_CATEGORY", "cd_subcategory|lib_subcategory|cd_category\nSC013|Filing\n Cabinets|C003\nSC014|Labels|C003\n", 2)
        self.assertEqual(rows[0]["lib_subcategory"], "Filing Cabinets")

    def test_each_remaining_dataset(self):
        examples = {
            "TYPE_CUSTOMER": "T001|Retail customer",
            "CUSTOMER": "A001|Smith|Ann||75001|Paris|France|T001",
            "PRODUCT": "P001|Widget|12,50|SC001",
            "SALE": "S001|13/12/2023|A001",
            "DETAIL_SALE": "S001|P001|2|12,50",
        }
        for dataset, row in examples.items():
            with self.subTest(dataset=dataset):
                self.run_case(dataset, "|".join(DATASETS[dataset].columns) + "\n" + row + "\n", 1)

    def test_malformed_row_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "bad.csv"
            source.write_text("sale_id|dt_sale|customer_id\nS1|2025-01-01\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                prepare_batch(source, Path(temp), "SALE", digest_file(source))

    def test_wrong_dataset_header_fails(self):
        with tempfile.TemporaryDirectory() as temp:
            source = Path(temp) / "bad.csv"
            source.write_text("a|b\nA|B\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                prepare_batch(source, Path(temp), "CATEGORY", digest_file(source))

    def test_pipe_quoted_field(self):
        rows = self.run_case("TYPE_CUSTOMER", "cd_type_customer|lib_type_customer\nT001|\"Retail|Office\"\n", 1)
        self.assertEqual(rows[0]["lib_type_customer"], "Retail|Office")


if __name__ == "__main__":
    unittest.main()
