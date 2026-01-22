import os
import json
import time
from pathlib import Path
from typing import List, Dict, Any

import requests


BASE_DIR = Path(__file__).resolve().parent.parent
NEWS_FILE = BASE_DIR / "news" / "top.json"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"

MODEL_NAME = "gpt-4o-mini"
MAX_ANALYZE_PER_RUN = 5


def load_news() -> List[Dict[str, Any]]:
    if not NEWS_FILE.exists():
        return []
    with NEWS_FILE.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    return []


def write_news(items: List[Dict[str, Any]]) -> None:
    tmp = NEWS_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, separators=(",", ":"))
    tmp.replace(NEWS_FILE)


def select_items_to_analyze(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result = []
    for item in items:
        analysis = item.get("analysis")
        if isinstance(analysis, dict):
            if analysis.get("version") == 1:
                continue
        result.append(item)
        if len(result) >= MAX_ANALYZE_PER_RUN:
            break
    return result


def build_prompt_for_item(item: Dict[str, Any]) -> List[Dict[str, str]]:
    headline = item.get("headline") or ""
    summary = item.get("summary") or ""
    source = item.get("source") or ""
    url = item.get("url") or ""
    related = item.get("relatedSymbols") or []
    related_text = ", ".join(related) if isinstance(related, list) else ""
    content = (
        "You are a financial assistant. Analyze the following market news and respond "
        "with a strict JSON object in English using this schema:\n"
        "{\n"
        '  "version": 1,\n'
        '  "sentiment": "bullish" | "bearish" | "neutral",\n'
        '  "confidence": number between 0 and 1,\n'
        '  "summary": "short explanation of the news content and key points",\n'
        '  "impact": "short description of potential impact on related symbols and market",\n'
        '  "risks": ["risk1", "risk2"]\n'
        "}\n"
        "Only output JSON, no extra text.\n\n"
        f"Headline: {headline}\n"
        f"Summary: {summary}\n"
        f"Source: {source}\n"
        f"URL: {url}\n"
        f"Related symbols: {related_text}\n"
    )
    return [
        {"role": "system", "content": "You are a precise financial news analyst."},
        {"role": "user", "content": content},
    ]


def call_openai(messages: List[Dict[str, str]]) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not set")
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MODEL_NAME,
        "messages": messages,
        "temperature": 0.2,
        "response_format": {"type": "json_object"},
    }
    resp = requests.post(OPENAI_API_URL, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError("No choices in OpenAI response")
    message = choices[0].get("message") or {}
    content = message.get("content") or ""
    return json.loads(content)


def update_items_with_analysis(
    items: List[Dict[str, Any]],
    analyzed: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    now = int(time.time())
    result = []
    for item in items:
        item_id = str(item.get("id"))
        analysis = analyzed.get(item_id)
        if analysis is not None:
            analysis["version"] = 1
            analysis["generatedAt"] = now
            item["analysis"] = analysis
        result.append(item)
    return result


def main() -> None:
    items = load_news()
    if not items:
        return
    targets = select_items_to_analyze(items)
    if not targets:
        return
    analyzed: Dict[str, Dict[str, Any]] = {}
    for item in targets:
        item_id = str(item.get("id"))
        try:
            messages = build_prompt_for_item(item)
            analysis = call_openai(messages)
            analyzed[item_id] = analysis
            time.sleep(1.0)
        except Exception:
            continue
    if not analyzed:
        return
    updated = update_items_with_analysis(items, analyzed)
    write_news(updated)


if __name__ == "__main__":
    main()
