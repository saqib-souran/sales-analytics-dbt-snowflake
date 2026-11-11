"""Allowlisted seven CSV dataset schemas for original ICOMMERCE Bronze ingestion."""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DatasetSpec:
    columns: tuple[str, ...]
    target_table: str
    multiline_repair: bool = False
    incremental_key: str | None = None


DATASETS: dict[str, DatasetSpec] = {
    "CATEGORY": DatasetSpec(
        ("cd_category", "lib_category"), "CATEGORY", True
    ),
    "SUB_CATEGORY": DatasetSpec(
        ("cd_subcategory", "lib_subcategory", "cd_category"),
        "SUB_CATEGORY", True,
    ),
    "TYPE_CUSTOMER": DatasetSpec(
        ("cd_type_customer", "lib_type_customer"),
        "TYPE_CUSTOMER",
    ),
    "CUSTOMER": DatasetSpec(
        ("customer_id", "last_name", "first_name", "address",
         "postal_code", "city", "country", "cd_type_customer"),
        "CUSTOMER",
        incremental_key="customer_id",
    ),
    "PRODUCT": DatasetSpec(
        ("cd_product", "lib_product", "price_product", "cd_subcategory"),
        "PRODUCT",
        incremental_key="cd_product",
    ),
    "SALE": DatasetSpec(
        ("sale_id", "dt_sale", "customer_id"), "SALE"
    ),
    "DETAIL_SALE": DatasetSpec(
        ("sale_id", "cd_product", "quantity_sold", "sale_price"),
        "DETAIL_SALE",
    ),
}


def get_dataset(name: str) -> DatasetSpec:
    if not isinstance(name, str) or name not in DATASETS:
        raise ValueError(f"Unknown dataset {name!r}; expected one of {', '.join(DATASETS)}")
    return DATASETS[name]


def dataset_from_filename(filename: str | Path) -> str:
    """Match the dataset name or an uppercase dataset prefix followed by '_'.

    Examples: CATEGORY.csv, category_batch_007.csv, SUB_CATEGORY_2027.csv.
    Longest names first prevent 'SALE' matching 'DETAIL_SALE'.
    """
    name = Path(filename).name
    if not name.lower().endswith(".csv"):
        raise ValueError("Expected a .csv file")
    stem = name[:-4].upper()
    for dataset in sorted(DATASETS, key=len, reverse=True):
        if stem == dataset or stem.startswith(dataset + "_"):
            return dataset
    raise ValueError(f"Unknown dataset filename: {name}")
