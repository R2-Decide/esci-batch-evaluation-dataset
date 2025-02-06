"""
Fetch, validate, and store products for a specific category
"""

import argparse
import asyncio
import json
import logging
import os
from typing import Dict, Iterator, List, Tuple

import aiohttp
from aiohttp import ClientError
from tqdm import tqdm

from dataset.product_config import ProductCategory

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def append_to_json(file_path: str, data: List[Dict]) -> None:
    """Append data to a JSON file as JSON lines"""
    with open(file_path, "a", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")


def load_from_json(file_path: str) -> Iterator[Dict]:
    """Load data from a JSON file"""
    if not os.path.exists(file_path):
        logger.error("Cannot find %s", file_path)
        return
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


async def validate_image_url(
    session: aiohttp.ClientSession, product: Dict
) -> Tuple[bool, Dict]:
    """Validate if an image URL is actually downloadable and return product info if valid"""
    if not (image_url := product.get("image")) or not (asin := product.get("asin")):
        return False, {}

    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with session.get(image_url, timeout=timeout) as response:
            if response.status == 200:
                # Try to get the first chunk of the image (1024 bytes)
                try:
                    async for _ in response.content.iter_chunked(1024):
                        return True, {
                            "asin": asin,
                            "title": product.get("title", ""),
                            "image_url": image_url,
                            "category": product.get("category", []),
                            "price": product.get("price", ""),
                            "stars": product.get("stars", ""),
                            "ratings": product.get("ratings", ""),
                            "attrs": product.get("attrs", {}),
                            "bullets": product.get("bullets", []),
                            "description": product.get("description", ""),
                            "info": product.get("info", {}),
                            "reviews": product.get("reviews", []),
                            "locale": product.get("locale", ""),
                        }
                except ClientError:
                    return False, {}
            else:
                return False, {}
    except (ClientError, asyncio.TimeoutError) as e:
        logger.error("Error validating %s: %s", asin, str(e))
    return False, {}


async def process_category(category: str, locale: str) -> int:
    """Process products for a given category and return count of valid products"""
    valid_count = 0

    logger.info("Loading products for category: %s...", category)
    json_path = "esci-s/esci.json"

    product_file = (
        f"output/{category.lower().replace(' & ', '_').replace(' ', '_')}.json"
    )
    valid_asins_file = f"output/valid_asins_{category.lower().replace(' & ', '_').replace(' ', '_')}.json"
    async with aiohttp.ClientSession() as session:
        tasks = []
        batch_size = 500
        batch = []
        asins_batch = []
        semaphore = asyncio.Semaphore(1000)  # Control concurrency

        async def sem_validate(product):
            async with semaphore:
                return await validate_image_url(session, product)

        for product in tqdm(load_from_json(json_path), desc="Processing products"):
            if (
                product.get("category")
                and product["category"][0] == category
                and product.get("locale") == locale
            ):
                tasks.append(asyncio.create_task(sem_validate(product)))
                if len(tasks) >= batch_size:
                    results = await asyncio.gather(*tasks)
                    for success, product_info in results:
                        if success:
                            batch.append(product_info)
                            asins_batch.append(product_info["asin"])
                            valid_count += 1
                    append_to_json(product_file, batch)
                    append_to_json(valid_asins_file, asins_batch)
                    batch = []
                    asins_batch = []
                    tasks = []
        if tasks:
            results = await asyncio.gather(*tasks)
            for success, product_info in results:
                if success:
                    batch.append(product_info)
                    asins_batch.append(product_info["asin"])
                    valid_count += 1
            append_to_json(product_file, batch)
            append_to_json(valid_asins_file, asins_batch)

    return valid_count


async def main():
    """Fetch, validate, and store products for a specific category"""
    parser = argparse.ArgumentParser(
        description="Validate product images for a specific category"
    )
    parser.add_argument(
        "category",
        type=str,
        choices=[cat.name for cat in ProductCategory],
        help="Product category to process (use enum name, e.g., ELECTRONICS, BOOKS, etc.)",
    )
    parser.add_argument(
        "--locale",
        type=str,
        choices=["us", "es", "jp"],
        default="us",
        help="Product locale (us/es/jp)",
    )

    args = parser.parse_args()

    category_value = ProductCategory[args.category].value

    valid_products = await process_category(category_value, args.locale)

    logger.info("Validation complete!")
    logger.info("Successfully validated products: %d", valid_products)


if __name__ == "__main__":
    asyncio.run(main())
