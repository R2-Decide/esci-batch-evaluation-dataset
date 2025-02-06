from typing import Dict, List

import requests
from tqdm import tqdm

from benchmarks.utils import load_json, save_json


async def benchmark_shopify_queries(
    shop_url: str, access_token: str, queries: List[Dict], count: int = 25
):
    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }

    results = []
    for query in tqdm(queries):
        url = f"https://{shop_url}/admin/api/2024-01/products.json"
        params = {"query": query["query"], "limit": count}

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        results.append(
            {
                "query_id": query["query_id"],
                "query": query["query"],
                "response": response.json()["products"],
            }
        )

    return results


async def main():
    SHOP_URL = "your-shop.myshopify.com"
    ACCESS_TOKEN = "your-access-token"

    queries = await load_json("queries.json")
    results = await benchmark_shopify_queries(SHOP_URL, ACCESS_TOKEN, queries)
    await save_json("results.json", results)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
