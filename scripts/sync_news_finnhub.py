import os
import json
import time
from pathlib import Path
from typing import List, Dict, Any

import requests


BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_FILE = BASE_DIR / "news" / "top.json"

API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
BASE_URL = "https://finnhub.io/api/v1"

MAX_ITEMS = 50
MAX_AGE_SECONDS = 24 * 3600


def fetch_general_news() -> List[Dict[str, Any]]:
    if not API_KEY:
        raise RuntimeError("FINNHUB_API_KEY is not set")
    url = f"{BASE_URL}/news"
    params = {"category": "general", "token": API_KEY}
    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError("Unexpected response format from Finnhub /news")
    return data


def transform_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    now = int(time.time())
    result: List[Dict[str, Any]] = []
    for item in items:
        dt = item.get("datetime")
        if isinstance(dt, (int, float)):
            ts = int(dt)
        else:
            continue
        if now - ts > MAX_AGE_SECONDS:
            continue
        headline = item.get("headline") or ""
        url = item.get("url") or ""
        if not headline or not url:
            continue
        id_val = item.get("id")
        source = item.get("source") or ""
        summary = item.get("summary") or ""
        related = item.get("related") or []
        if not isinstance(related, list):
            related = []
        result.append(
            {
                "id": str(id_val) if id_val is not None else f"{ts}-{len(result)}",
                "headline": headline,
                "summary": summary,
                "url": url,
                "source": source,
                "relatedSymbols": related,
                "publishedAt": ts,
            }
        )
    result.sort(key=lambda x: x["publishedAt"], reverse=True)
    if len(result) > MAX_ITEMS:
        result = result[:MAX_ITEMS]
    return result


def write_json_atomic(path: Path, data: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    tmp.replace(path)


def main() -> None:
    items = fetch_general_news()
    transformed = transform_items(items)
    write_json_atomic(OUTPUT_FILE, transformed)


if __name__ == "__main__":
    main()
