#!/usr/bin/env python3
"""
update_chart.py
Queries Shopify orders for the past 7 days (Wednesday midnight to Wednesday midnight),
ranks the top 30 products by units sold (online + POS), and stores the result as a
shop metafield so the chart-weekly.liquid section can display it.

Required environment variables:
  SHOPIFY_SHOP   — e.g. spindizzy.myshopify.com
  SHOPIFY_TOKEN  — Admin API access token (needs read_orders + write_metafields)
"""

import os
import json
import requests
from datetime import datetime, timezone, timedelta

SHOP    = os.environ["SHOPIFY_SHOP"]
TOKEN   = os.environ["SHOPIFY_TOKEN"]
API_VER = "2024-01"
BASE    = f"https://{SHOP}/admin/api/{API_VER}"
HEADERS = {
    "X-Shopify-Access-Token": TOKEN,
    "Content-Type": "application/json",
}


def get_orders(since: datetime, until: datetime) -> list:
    """Paginate through all orders (any status) in the given date window."""
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
        # Follow pagination
        url = None
        params = {}
        for part in r.headers.get("Link", "").split(","):
            part = part.strip()
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
    return orders


def get_product(product_id: str) -> dict:
    """Fetch title, handle and first image for a product."""
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
    """Read the existing chart metafield so we can compute position changes."""
    r = requests.get(
        f"{BASE}/shop/metafields.json",
        headers=HEADERS,
        params={"namespace": "chart", "key": "weekly_top30"},
        timeout=15,
    )
    if r.status_code == 200:
        mfs = r.json().get("metafields", [])
        if mfs:
            try:
                return json.loads(mfs[0]["value"])
            except (KeyError, json.JSONDecodeError):
                pass
    return None


def upsert_metafield(value: dict) -> None:
    """Create or update the chart metafield."""
    r = requests.get(
        f"{BASE}/shop/metafields.json",
        headers=HEADERS,
        params={"namespace": "chart", "key": "weekly_top30"},
        timeout=15,
    )
    existing = r.json().get("metafields", [])
    payload = {
        "metafield": {
            "namespace": "chart",
            "key": "weekly_top30",
            "type": "json",
            "value": json.dumps(value),
        }
    }
    if existing:
        mf_id = existing[0]["id"]
        requests.put(
            f"{BASE}/shop/metafields/{mf_id}.json",
            headers=HEADERS,
            json=payload,
            timeout=15,
        ).raise_for_status()
    else:
        requests.post(
            f"{BASE}/shop/metafields.json",
            headers=HEADERS,
            json=payload,
            timeout=15,
        ).raise_for_status()


def main() -> None:
    now       = datetime.now(timezone.utc)
    week_end  = now
    week_start = now - timedelta(days=7)

    print(f"Chart period: {week_start.date()} → {week_end.date()}")

    # ── 1. Fetch orders ──────────────────────────────────────────────────────
    orders = get_orders(week_start, week_end)
    print(f"Orders found: {len(orders)}")

    # ── 2. Aggregate units sold per product ──────────────────────────────────
    sales: dict[str, int] = {}
    for order in orders:
        for item in order.get("line_items", []):
            pid = str(item.get("product_id") or "")
            if not pid or pid == "None":
                continue
            sales[pid] = sales.get(pid, 0) + int(item.get("quantity", 0))

    if not sales:
        print("No sales data found — chart not updated.")
        return

    # ── 3. Rank top 30 ───────────────────────────────────────────────────────
    ranked = sorted(sales.items(), key=lambda x: x[1], reverse=True)[:30]

    # ── 4. Previous positions for movement indicators ────────────────────────
    prev_chart = get_current_chart()
    prev_positions: dict[str, int] = {}
    if prev_chart and "chart" in prev_chart:
        for entry in prev_chart["chart"]:
            prev_positions[str(entry["product_id"])] = entry["position"]

    # ── 5. Build chart entries ───────────────────────────────────────────────
    chart_entries = []
    for position, (product_id, units) in enumerate(ranked, 1):
        product = get_product(product_id)
        if not product:
            print(f"  Skipping product {product_id} — not found")
            continue

        images = product.get("images", [])
        image  = images[0].get("src", "") if images else ""

        prev_pos = prev_positions.get(product_id)  # None = new entry

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
        print(f"  #{position:02d} {prev_label:12s} {units:4d} sold — {product.get('title', '?')}")

    # ── 6. Write metafield ───────────────────────────────────────────────────
    result = {
        "updated":    now.isoformat(),
        "week_start": week_start.isoformat(),
        "week_end":   week_end.isoformat(),
        "chart":      chart_entries,
    }
    upsert_metafield(result)
    print(f"\nChart metafield updated — {len(chart_entries)} entries.")


if __name__ == "__main__":
    main()
