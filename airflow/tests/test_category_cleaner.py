import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
from icommerce_ingestion.category_cleaner import prepare_category, parse_category_rows, verify_prepared_file


class CategoryTests(unittest.TestCase):
    def test_multiline_category_and_duplicate_kept(self):
        with tempfile.TemporaryDirectory() as folder:
            src = Path(folder) / "CATEGORY.csv"
            dst = Path(folder) / "out.csv"
            src.write_text(
                "cd_category|lib_category\n"
                "C001|Technology\n"
                "C002|  Furniture  \n"
                "C004|Other\t\t\n"
                "\tcategories\n"
                "C001|TECHNOLOGY\n",
                encoding="utf-8",
            )
            result = prepare_category(src, dst)
            verify_prepared_file(result)
            self.assertEqual(result["row_count"], 4)
            self.assertEqual(result["repeated_category_codes"], 1)
            self.assertIn("C004|Other categories", dst.read_text(encoding="utf-8"))

    def test_wrong_field_count_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            src = Path(folder) / "CATEGORY.csv"
            src.write_text("cd_category|lib_category\nC001|X|Y\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                parse_category_rows(src)


if __name__ == "__main__":
    unittest.main()
