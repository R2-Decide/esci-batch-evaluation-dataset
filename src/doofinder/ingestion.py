import json
import argparse
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import Dict, Any, List
import time


class DoofinderIngestion:
    def __init__(
        self, hashid: str, api_key: str, search_zone: str = "eu1", batch_size: int = 100
    ):
        self.hashid = hashid
        self.api_key = api_key
        self.search_zone = search_zone
        self.batch_size = batch_size
        self.base_url = f"https://{search_zone}-search.doofinder.com/6/{hashid}"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Token {api_key}",
        }

    def create_temporary_index(self, name: str) -> str:
        """Create a temporary index for bulk ingestion"""
        endpoint = f"{self.base_url}/indices/{name}_temp"
        response = requests.post(endpoint, headers=self.headers)
        response.raise_for_status()
        return f"{name}_temp"

    def ingest_batch(self, index_name: str, products: List[Dict[str, Any]]) -> Dict:
        """Ingest a batch of products"""
        endpoint = f"{self.base_url}/items/{index_name}"

        try:
            response = requests.post(
                endpoint, headers=self.headers, json=products, timeout=30
            )
            response.raise_for_status()
            return {"success": True, "data": response.json()}
        except Exception as e:
            return {"success": False, "error": str(e), "products": products}

    def replace_index(self, temp_index: str, final_index: str) -> None:
        """Replace the production index with the temporary index"""
        endpoint = f"{self.base_url}/indices/{temp_index}/replace/{final_index}"
        response = requests.post(endpoint, headers=self.headers)
        response.raise_for_status()


def transform_product(product: Dict[str, Any]) -> Dict[str, Any]:
    """Transform product data to Doofinder format"""
    return {
        "id": product.get(
            "platform_id", str(time.time())
        ),  # Fallback to timestamp if no ID
        "title": product["title"],
        "description": product.get("description", ""),
        "image_url": product.get("image_url", ""),
        "link": product.get("url", ""),
        "price": product.get("price", 0.0),
        "categories": product.get("category", []),
        "availability": "in stock",
    }


def main():
    parser = argparse.ArgumentParser(description="Bulk import products to Doofinder")
    parser.add_argument(
        "--hashid", required=True, help="Doofinder search engine hashid"
    )
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--file", required=True, help="Path to products JSON file")
    parser.add_argument(
        "--workers", type=int, default=5, help="Number of parallel workers"
    )
    parser.add_argument(
        "--batch-size", type=int, default=100, help="Products per batch"
    )
    parser.add_argument("--zone", default="eu1", help="Search zone (eu1, us1)")
    parser.add_argument("--index", default="product", help="Index name")

    args = parser.parse_args()

    try:
        # Initialize Doofinder client
        client = DoofinderIngestion(args.hashid, args.token, args.zone, args.batch_size)

        # Load products
        with open(args.file, "r") as f:
            products = json.load(f)

        # Transform products to Doofinder format
        transformed_products = [transform_product(p) for p in products]

        # Create batches
        batches = [
            transformed_products[i : i + args.batch_size]
            for i in range(0, len(transformed_products), args.batch_size)
        ]

        print(
            f"🚀 Starting import of {len(transformed_products)} products in {len(batches)} batches"
        )

        # Create temporary index
        temp_index = client.create_temporary_index(args.index)
        print(f"📦 Created temporary index: {temp_index}")

        success_count = 0
        error_count = 0
        error_batches = []

        # Process batches with parallel workers
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(client.ingest_batch, temp_index, batch)
                for batch in batches
            ]

            with tqdm(
                total=len(batches), desc="Ingesting batches", unit="batch"
            ) as pbar:
                for future in as_completed(futures):
                    result = future.result()

                    if result["success"]:
                        success_count += len(result["data"])
                    else:
                        error_count += len(result["products"])
                        error_batches.append(result)

                    pbar.update(1)
                    pbar.set_postfix_str(f"✅ {success_count} | ❌ {error_count}")

        # Replace production index if we have successful imports
        if success_count > 0:
            print("\n🔄 Replacing production index...")
            client.replace_index(temp_index, args.index)
            print("✅ Index replaced successfully")

        print(f"\n🎉 Import complete!")
        print(f"Total products: {len(transformed_products)}")
        print(f"Successful imports: {success_count}")
        print(f"Failed imports: {error_count}")

        if error_batches:
            print("\n⚠️  Error details:")
            for batch in error_batches:
                print(f"\nError: {batch['error']}")
                print(f"Failed products in batch: {len(batch['products'])}")

    except Exception as e:
        print(f"🔥 Critical error: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()
