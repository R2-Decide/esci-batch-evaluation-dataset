import argparse
import requests
import json
from typing import Dict, Any, Optional, List
from uuid import uuid4


class DoofinderSearch:
    def __init__(self, hashid: str, api_key: str, search_zone: str = "eu1"):
        self.hashid = hashid
        self.api_key = api_key
        self.search_zone = search_zone
        self.base_url = f"https://{search_zone}-search.doofinder.com/6/{hashid}"
        self.headers = {"Authorization": f"Token {api_key}"}

    def search(
        self,
        query: str,
        page: int = 1,
        rpp: int = 20,
        indices: Optional[List[str]] = None,
        filters: Optional[Dict] = None,
        sort: Optional[List[Dict]] = None,
        facets: Optional[List[Dict]] = None,
        query_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Perform a search query

        Args:
            query: Search term
            page: Page number (default: 1)
            rpp: Results per page (default: 20)
            indices: List of indices to search in (e.g. ['product'])
            filters: Filter parameters (e.g. {'price': {'gte': 100, 'lt': 200}})
            sort: Sort parameters (e.g. [{'price': 'asc'}])
            facets: Facet parameters (e.g. [{'field': 'brand', 'size': 10}])
            query_name: Type of query ('match_and', 'match_or', 'fuzzy')
        """
        params = {
            "query": query,
            "page": page,
            "rpp": rpp,
            "session_id": str(uuid4()),  # Generate unique session ID
        }

        # Add optional parameters
        if indices:
            params["indices[]"] = indices

        if filters:
            for field, conditions in filters.items():
                if isinstance(conditions, dict):
                    for op, value in conditions.items():
                        params[f"filter[{field}][{op}]"] = value
                else:
                    params[f"filter[{field}][]"] = conditions

        if sort:
            for idx, sort_param in enumerate(sort):
                for field, direction in sort_param.items():
                    params[f"sort[{idx}][{field}]"] = direction

        if facets:
            for idx, facet in enumerate(facets):
                for key, value in facet.items():
                    params[f"facets[{idx}][{key}]"] = value

        if query_name:
            params["query_name"] = query_name

        response = requests.get(
            f"{self.base_url}/_search", headers=self.headers, params=params
        )
        response.raise_for_status()
        return response.json()

    def suggest(self, query: str, indices: Optional[List[str]] = None) -> List[str]:
        """Get search suggestions for a query"""
        params = {"query": query, "session_id": str(uuid4())}

        if indices:
            params["indices[]"] = indices

        response = requests.get(
            f"{self.base_url}/_suggest", headers=self.headers, params=params
        )
        response.raise_for_status()
        return response.json()

    def similar_products(
        self,
        product_id: str,
        rpp: int = 10,
        exclude_self: bool = True,
        filters: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Find visually similar products"""
        params = {
            "dfid": product_id,
            "rpp": rpp,
            "exclude_self": exclude_self,
            "session_id": str(uuid4()),
        }

        if filters:
            for field, conditions in filters.items():
                if isinstance(conditions, dict):
                    for op, value in conditions.items():
                        params[f"filter[{field}][{op}]"] = value
                else:
                    params[f"filter[{field}][]"] = conditions

        response = requests.get(
            f"{self.base_url}/_visually_similar", headers=self.headers, params=params
        )
        response.raise_for_status()
        return response.json()


def print_results(results: Dict[str, Any], show_facets: bool = True):
    """Pretty print search results"""
    print(f"\nFound {results['total']} results")
    print("\nItems:")
    for idx, item in enumerate(results["results"], 1):
        print(f"\n{idx}. {item['title']}")
        if "description" in item:
            print(f"   Description: {item['description'][:100]}...")
        if "price" in item:
            print(f"   Price: {item['price']}")
        print(f"   ID: {item['id']}")

    if show_facets and "facets" in results and results["facets"]:
        print("\nFacets:")
        for facet in results["facets"]:
            if "terms" in facet:
                print(f"\n{facet['key']}:")
                for term in facet["terms"]["items"]:
                    print(f"  - {term['name']} ({term['count']})")


def main():
    parser = argparse.ArgumentParser(description="Search products in Doofinder")
    parser.add_argument(
        "--hashid", required=True, help="Doofinder search engine hashid"
    )
    parser.add_argument("--token", required=True, help="API token")
    parser.add_argument("--zone", default="eu1", help="Search zone (eu1, us1)")
    parser.add_argument("--query", required=True, help="Search query")
    parser.add_argument("--page", type=int, default=1, help="Page number")
    parser.add_argument("--rpp", type=int, default=20, help="Results per page")
    parser.add_argument("--index", help="Index to search in (e.g. product)")
    parser.add_argument(
        "--filter", help='JSON filter string (e.g. \'{"price":{"gte":100}}\')'
    )
    parser.add_argument("--sort", help='JSON sort string (e.g. \'[{"price":"asc"}]\')')
    parser.add_argument(
        "--suggest", action="store_true", help="Get search suggestions instead"
    )

    args = parser.parse_args()

    try:
        client = DoofinderSearch(args.hashid, args.token, args.zone)

        if args.suggest:
            # Get suggestions
            suggestions = client.suggest(
                args.query, indices=[args.index] if args.index else None
            )
            print("\nSuggestions:")
            for suggestion in suggestions:
                print(f"- {suggestion}")
        else:
            # Perform search
            filters = json.loads(args.filter) if args.filter else None
            sort = json.loads(args.sort) if args.sort else None

            results = client.search(
                query=args.query,
                page=args.page,
                rpp=args.rpp,
                indices=[args.index] if args.index else None,
                filters=filters,
                sort=sort,
            )

            print_results(results)

    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1)


if __name__ == "__main__":
    main()
