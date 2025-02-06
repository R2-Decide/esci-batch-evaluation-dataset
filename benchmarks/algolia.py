from typing import Dict, List

from algoliasearch.search.client import SearchClient
from tqdm import tqdm

from benchmarks.utils import load_json, save_json


async def benchmark_algolia_queries(
    client: SearchClient, index_name: str, queries: List[Dict], count: int = 25
):
    results = []
    for query in tqdm(queries):
        response = await client.search(
            search_method_params={
                "requests": [
                    {
                        "indexName": index_name,
                        "query": query["query"],
                        "hitsPerPage": count,
                    }
                ]
            }
        )
        results.append(
            {
                "query_id": query["query_id"],
                "query": query["query"],
                "response": response.results
            }
        )
    return results


async def main():
    ALGOLIA_APP_ID = "YourAlgoliaAppId"
    ALGOLIA_API_KEY = "YourAlgoliaApiKey"
    INDEX_NAME = "YourIndexName"

    client = SearchClient(ALGOLIA_APP_ID, ALGOLIA_API_KEY)

    queries = await load_json("queries.json")

    results = benchmark_algolia_queries(client, INDEX_NAME, queries)

    await save_json("results.json", results)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
