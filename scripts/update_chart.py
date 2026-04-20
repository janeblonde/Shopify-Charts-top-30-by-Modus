#!/usr/bin/env python3
"""
update_chart.py
Queries Shopify orders for the past 7 days, ranks the top 30 products by units
sold, and commits the result as chart-data.json in the GitHub repository so the
Shopify storefront can fetch it directly.

Required environment variables:
  SHOPIFY_SHOP    -- e.g. spindizzy.myshopify.com
  SHOPIFY_TOKEN   -- Admin API access token (needs read_orders + read_products)
  GITHUB_TOKEN    -- Provided automatically by GitHub Actions
  GITHUB_REPO     -- e.g. janeblonde/Shopify-Charts-top-30-by-Modus
"""

import os
import json
import base64
import requests
from datetime import datetime, timezone, timedelta

SHOP      = os.environ["SHOPIFY_SHOP"]
TOKEN     = os.environ["SHOPIFY_TOKEN"]
GH_TOKEN  = os.environ["GITHUB_TOKEN"]
GH_REPO   = os.environ.get("GITHUB_REPO", "janeblonde/Shopify-Charts-top-30-by-Modus")
API_VER   = "2024-01"
BASE      = f"https://{SHOP}/admin/api/{API_VER}"
HEADERS   = {
    "X-Shopify-Access-Token": TOKEN,
    "Content-Type": "application/json",
}
GH_HEADERS = {
    "Authorization": f"token {GH_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}
GH_FILE_PATH = "chart-data.json"

# Collections to exclude from the chart (by handle)
EXCLUDE_COLLECTIONS = [
    "events",
    "hi-fi-accessories",
    "accessories",
    "sleeves",
    "gift-cards",
]


def get_excluded_product_ids() -> set:
    """Return a set of product IDs belonging to any excluded collection."""
    excluded = set()
    for handle in EXCLUDE_COLLECTIONS:
        collection_id = None
        # Check custom collections
        for ctype in ("custom_collections", "smart_collections"):
            r = requests.get(
                f"{BASE}/{ctype}.json",
                headers=HEADERS,
                params={"handle": handle, "fields": "id"},
                timeout=15,
            )
            if r.status_code == 200:
                cols = r.json().get(ctype.replace("custom_", "").replace("smart_", ""), [])
                # key name differs: custom_collections -> custom_collections, smart_collections -> smart_collections
                key = "custom_collections" if ctype == "custom_collections" else "smart_collections"
                cols = r.json().get(key, [])
                if cols:
                    collection_id = cols[0]["id"]
                    break
        if not collection_id:
            print(f"  Collection not found: {handle}")
            continue
        # Paginate through all products in this collection
        url = f"{BASE}/collections/{collection_id}/products.json"
        params = {"limit": 250, "fields": "id"}
        while url:
            r = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if r.status_code != 200:
                break
            for p in r.json().get("products", []):
                excluded.add(str(p["id"]))
            url = None
            params = {}
            for part in r.headers.get("Link", "").split(","):
                part = part.strip()
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
        print(f"  Excluded collection '{handle}': {len(excluded)} products so far")
    return excluded


def get_orders(since: datetime, until: datetime) -> list:
    orders = []
    url = f"{BASE}/orders.json"
    params = {
        "status": "any",
        "created_at_min": since.isoformat(),
        "created_at_max": until.isoformat(),
        "limit": 250,
        "fields": "line_items",
    }
    while url:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        orders.extend(r.json().get("orders", []))
        url = None
        params = {}
        for part in r.headers.get("Link", "").split(","):
            part = part.strip()
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
    return orders


def get_product(product_id: str) -> dict:
    r = requests.get(
        f"{BASE}/products/{product_id}.json",
        headers=HEADERS,
        params={"fields": "id,title,handle,images"},
        timeout=15,
    )
    if r.status_code == 200:
        return r.json().get("product", {})
    return {}


def get_current_chart() -> dict | None:
    """Read the existing chart-data.json from GitHub to get previous positions."""
    r = requests.get(
        f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}",
        headers=GH_HEADERS,
        timeout=15,
    )
    if r.status_code == 200:
        try:
            content = base64.b64decode(r.json()["content"]).decode("utf-8")
            return json.loads(content)
        except Exception:
            pass
    return None


def commit_chart(data: dict, current_file: dict | None) -> None:
    """Create or update chart-data.json in the GitHub repo."""
    content = base64.b64encode(json.dumps(data, indent=2).encode("utf-8")).decode("utf-8")
    payload = {
        "message": f"Update chart data {datetime.now(timezone.utc).strftime('%Y-%m-%d')}",
        "content": content,
    }
    # If file already exists we need its SHA to update it
    if current_file:
        r = requests.get(
            f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}",
            headers=GH_HEADERS,
            timeout=15,
        )
        if r.status_code == 200:
            payload["sha"] = r.json()["sha"]

    r = requests.put(
        f"https://api.github.com/repos/{GH_REPO}/contents/{GH_FILE_PATH}",
        headers=GH_HEADERS,
        json=payload,
        timeout=30,
    )
    r.raise_for_status()


def main() -> None:
    now        = datetime.now(timezone.utc)
    week_end   = now
    week_start = now - timedelta(days=7)

    print(f"Chart period: {week_start.date()} -> {week_end.date()}")

    print("Building exclusion list...")
    excluded_ids = get_excluded_product_ids()
    print(f"Excluding {len(excluded_ids)} products from {len(EXCLUDE_COLLECTIONS)} collections")

    orders = get_orders(week_start, week_end)
    print(f"Orders found: {len(orders)}")

    sales: dict[str, int] = {}
    for order in orders:
        for item in order.get("line_items", []):
            pid = str(item.get("product_id") or "")
            if not pid or pid == "None":
                continue
            if pid in excluded_ids:
                continue
            sales[pid] = sales.get(pid, 0) + int(item.get("quantity", 0))

    if not sales:
        print("No sales data found -- chart not updated.")
        return

    ranked = sorted(sales.items(), key=lambda x: x[1], reverse=True)[:30]

    current_chart = get_current_chart()
    prev_positions: dict[str, int] = {}
    if current_chart and "chart" in current_chart:
        for entry in current_chart["chart"]:
            prev_positions[str(entry["product_id"])] = entry["position"]

    chart_entries = []
    for position, (product_id, units) in enumerate(ranked, 1):
        product = get_product(product_id)
        if not product:
            print(f"  Skipping product {product_id} -- not found")
            continue

        images = product.get("images", [])
        image  = images[0].get("src", "") if images else ""

        prev_pos = prev_positions.get(product_id)

        entry = {
            "position":          position,
            "previous_position": prev_pos,
            "product_id":        product_id,
            "handle":            product.get("handle", ""),
            "title":             product.get("title", ""),
            "image":             image,
            "units":             units,
        }
        chart_entries.append(entry)
        prev_label = f"(was #{prev_pos})" if prev_pos else "(NEW)"
        print(f"  #{position:02d} {prev_label:12s} {units:4d} sold -- {product.get('title', '?')}")

    result = {
        "updated":    now.isoformat(),
        "week_start": week_start.isoformat(),
        "week_end":   week_end.isoformat(),
        "chart":      chart_entries,
    }

    commit_chart(result, current_chart)
    print(f"\nChart committed to GitHub -- {len(chart_entries)} entries.")


if __name__ == "__main__":
    main()