import asyncio
from typing import Dict, List

import pydoof
from tqdm import tqdm

from benchmarks.utils import load_json, save_json


def benchmark_doofinder_queries(hash_id: str, queries: List[Dict], count: int = 25):
    results = []
    for query in tqdm(queries):
        response = pydoof.search.query(
            hashid=hash_id,
            query=query["query"],
            rpp=count,
        )
        results.append(
            {
                "query_id": query["query_id"],
                "query": query["query"],
                "response": response.results,
            }
        )
    return results


async def main():
    DOOFINDER_TOKEN = "b8bcb..."
    DOOFINDER_SEARCH_URL = "https://eu1-search.doofinder.com"
    DOOFINDER_HASH_ID = "abe16c8..."

    pydoof.token = DOOFINDER_TOKEN
    pydoof.search_url = DOOFINDER_SEARCH_URL

    queries = await load_json("queries.json")

    results = await asyncio.to_thread(
        benchmark_doofinder_queries, DOOFINDER_HASH_ID, queries
    )

    await save_json("results.json", results)


if __name__ == "__main__":
    asyncio.run(main())
