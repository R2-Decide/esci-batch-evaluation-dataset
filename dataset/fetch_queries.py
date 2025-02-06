"""
Fetch queries for products in a specific category
"""

import argparse
import asyncio
import json
import logging
import os

import aiofiles
import pandas as pd
from tqdm import tqdm

from dataset.product_config import ProductCategory

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def load_valid_asins(category: str) -> list:
    """Load valid ASINs from the output directory"""
    valid_asins_file = f"output/valid_asins_{category.lower().replace(' & ', '_').replace(' ', '_')}.json"

    asins = []
    try:
        async with aiofiles.open(valid_asins_file, "r", encoding="utf-8") as f:
            async for line in f:
                asins.append(json.loads(line))
    except FileNotFoundError:
        logger.error("No valid ASINs file found at %s", valid_asins_file)
        logger.error("Please run fetch_products.py first!")

    return asins


async def fetch_valid_queries(valid_asins: list, locale: str, labels: list) -> list:
    """Fetch queries related to valid ASINs for the given category"""
    base_path = "esci-data/shopping_queries_dataset"

    logger.info("Reading parquet files...")
    try:
        df_examples = pd.read_parquet(
            f"{base_path}/shopping_queries_dataset_examples.parquet"
        )
    except Exception as e:
        logger.error("Error reading parquet files: %s", e)
        return []

    logger.info("Filtering products...")
    # Filter products by valid ASINs, locale and labels
    df_examples_filtered = df_examples[
        (df_examples["product_id"].isin(valid_asins))
        & (df_examples["product_locale"] == locale)
        & (df_examples["esci_label"].isin(labels))
        & (df_examples["split"] == "train")
        & (df_examples["small_version"] == 1)
    ]

    logger.info("Grouping queries...")
    # Group by query to get all products for each query
    query_groups = (
        df_examples_filtered.groupby("query_id", sort=False)
        .agg(
            {
                "query": "first",  # Get the query text
                "product_id": list,  # Get list of product IDs
                "esci_label": list,  # Get list of ESCI labels
                "product_locale": list,  # Get list of product locales
            }
        )
        .reset_index()
    )

    # Create the output list with progress bar
    queries_list = []
    for _, row in tqdm(
        query_groups.iterrows(), desc="Processing queries", total=len(query_groups)
    ):
        queries_list.append(
            {
                "query": row["query"],
                "query_id": row["query_id"],
                "product_asins": row["product_id"],
                "esci_labels": row["esci_label"],
                "product_locales": row["product_locale"],
            }
        )

    return queries_list


async def save_results(
    category: str, queries_list: list, locale: str, labels: list
) -> None:
    """Save the query results"""
    os.makedirs("output", exist_ok=True)

    # Clean category name for filename
    category_filename = category.lower().replace(" & ", "_").replace(" ", "_")
    labels_str = "".join(sorted(labels))

    # Save queries with their products
    queries_file = f"output/{category_filename}_{locale}_{labels_str}_queries.json"
    async with aiofiles.open(queries_file, "w", encoding="utf-8") as f:
        await f.write(json.dumps(queries_list, indent=2))

    logger.info("\nResults saved:")
    logger.info("- Queries data: %s", queries_file)
    logger.info("Total queries found: %d", len(queries_list))


async def main():
    """
    Fetch queries for products in a specific category
    """
    parser = argparse.ArgumentParser(
        description="Fetch queries for products in a specific category"
    )
    parser.add_argument(
        "category",
        type=str,
        choices=[cat.name for cat in ProductCategory],
        help="Product category to process",
    )
    parser.add_argument(
        "--locale",
        type=str,
        choices=["us", "es", "jp"],
        default="us",
        help="Product locale (us/es/jp)",
    )
    parser.add_argument(
        "--labels",
        nargs="+",
        type=str,
        choices=["E", "S", "C", "I"],
        default=["E", "S", "C", "I"],
        help="ESCI labels to include",
    )

    args = parser.parse_args()

    # Convert enum name to actual category value
    category_value = ProductCategory[args.category].value

    # Load valid ASINs
    logger.info("Loading valid ASINs for category: %s", category_value)
    valid_asins = await load_valid_asins(category_value)

    if not valid_asins:
        return

    logger.info("\nFound %d valid ASINs", len(valid_asins))
    logger.info("Fetching associated queries for locale: %s", args.locale)
    logger.info("Including labels: %s", ", ".join(args.labels))

    # Fetch and save queries
    queries_list = await fetch_valid_queries(valid_asins, args.locale, args.labels)
    await save_results(category_value, queries_list, args.locale, args.labels)


if __name__ == "__main__":
    asyncio.run(main())
