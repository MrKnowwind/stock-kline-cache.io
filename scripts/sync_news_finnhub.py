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

MAX_ITEMS = 1000
MAX_AGE_SECONDS = 365 * 24 * 3600


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


def load_existing() -> List[Dict[str, Any]]:
    if not OUTPUT_FILE.exists():
        return []
    try:
        with OUTPUT_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return []
    except Exception:
        return []


def transform_raw_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
        image = item.get("image") or ""
        entry_id = str(id_val) if id_val is not None else f"{ts}-{len(result)}"
        result.append(
            {
                "id": entry_id,
                "headline": headline,
                "summary": summary,
                "url": url,
                "source": source,
                "image": image,
                "relatedSymbols": related,
                "publishedAt": ts,
            }
        )
    return result


def merge_and_trim(
    existing: List[Dict[str, Any]],
    new_items: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    now = int(time.time())
    merged: Dict[str, Dict[str, Any]] = {}
    for item in existing:
        ts = item.get("publishedAt")
        if not isinstance(ts, int):
            continue
        if now - ts > MAX_AGE_SECONDS:
            continue
        entry_id = str(item.get("id") or "")
        if not entry_id:
            continue
        merged[entry_id] = item
    for item in new_items:
        ts = item.get("publishedAt")
        if not isinstance(ts, int):
            continue
        if now - ts > MAX_AGE_SECONDS:
            continue
        entry_id = str(item.get("id") or "")
        if not entry_id:
            continue
        existing_item = merged.get(entry_id)
        if existing_item is not None:
            analysis = existing_item.get("analysis")
            if analysis is not None and item.get("analysis") is None:
                item["analysis"] = analysis
        merged[entry_id] = item
    result = list(merged.values())
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
    existing = load_existing()
    raw_items = fetch_general_news()
    transformed_new = transform_raw_items(raw_items)
    merged = merge_and_trim(existing, transformed_new)
    write_json_atomic(OUTPUT_FILE, merged)


if __name__ == "__main__":
    main()
