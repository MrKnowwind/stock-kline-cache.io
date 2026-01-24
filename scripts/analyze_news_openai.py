import json
import os
import time
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from readability import Document
from openai import OpenAI


NEWS_FILE_PATH = os.getenv("NEWS_FILE_PATH", "news/top.json")
MAX_ARTICLES_PER_RUN = int(os.getenv("MAX_ARTICLES_PER_RUN", "50"))
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
ANALYSIS_VERSION = 2


def load_news(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_news(path: str, items: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)


def fetch_article_html(url: str, timeout: int = 10) -> Optional[str]:
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code != 200:
            return None
        return resp.text
    except Exception:
        return None


def extract_main_text(html: str) -> Optional[str]:
    try:
        doc = Document(html)
        summary_html = doc.summary()
        soup = BeautifulSoup(summary_html, "html.parser")
        text = " ".join(s.strip() for s in soup.stripped_strings)
        if not text:
            soup_full = BeautifulSoup(html, "html.parser")
            text = " ".join(s.strip() for s in soup_full.stripped_strings)
        return text or None
    except Exception:
        return None


def truncate_text(text: str, max_chars: int = 6000) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars]


def build_prompt(item: Dict[str, Any], article_text: Optional[str]) -> str:
    headline = item.get("headline") or ""
    summary = item.get("summary") or ""
    source = item.get("source") or ""
    related = item.get("relatedSymbols") or []
    url = item.get("url") or ""

    base_info = [
        f"Title: {headline}",
        f"Source: {source}",
        f"URL: {url}",
        f"Related symbols: {', '.join(related) if related else 'N/A'}",
        "",
        f"Provider summary: {summary}",
    ]

    if article_text:
        base_info.append("")
        base_info.append("Full article text:")
        base_info.append(article_text)

    info_block = "\n".join(base_info)

    prompt = (
        "You are a professional equity research analyst.\n"
        "Based on the following news article, provide a detailed, "
        "actionable analysis for stock traders.\n\n"
        f"{info_block}\n\n"
        "Return a JSON object with exactly these fields:\n"
        "- sentiment: one of ['bullish', 'bearish', 'neutral']\n"
        "- confidence: a number between 0 and 1\n"
        "- summary: in English, 3-6 sentences, clearly explaining the key "
        "events, background, and the logical chain from the news to the "
        "business fundamentals or industry context.\n"
        "- impact: in English, 2-4 sentences, concretely describing the "
        "potential impact on the related stocks. Cover short-term and/or "
        "medium-term effects, and mention drivers such as earnings outlook, "
        "valuation, sentiment, liquidity, or macro factors when relevant.\n"
        "- risks: an array of English strings. Each element is one specific "
        "risk or uncertainty (for example: 'regulatory approval risk', "
        "'integration risk', 'demand slowdown risk'). Prefer 2-4 concise "
        "and concrete items when possible.\n"
    )

    return prompt


def create_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    return OpenAI(api_key=api_key)


def analyze_with_openai(client: OpenAI, prompt: str) -> Dict[str, Any]:
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a professional equity research analyst. "
                    "Always respond with a single JSON object and use English."
                ),
            },
            {"role": "user", "content": prompt},
        ],
    )
    content = resp.choices[0].message.content
    data = json.loads(content)

    sentiment = str(data.get("sentiment", "neutral")).lower()
    if sentiment not in ["bullish", "bearish", "neutral"]:
        sentiment = "neutral"

    try:
        confidence = float(data.get("confidence", 0.5))
    except Exception:
        confidence = 0.5
    confidence = max(0.0, min(1.0, confidence))

    summary = str(data.get("summary", "")).strip()
    impact = str(data.get("impact", "")).strip()
    risks_raw = data.get("risks", [])
    if isinstance(risks_raw, list):
        risks = [str(r).strip() for r in risks_raw if str(r).strip()]
    elif isinstance(risks_raw, str) and risks_raw.strip():
        risks = [r.strip() for r in risks_raw.split("\n") if r.strip()]
    else:
        risks = []

    return {
        "sentiment": sentiment,
        "confidence": confidence,
        "summary": summary,
        "impact": impact,
        "risks": risks,
    }


def build_analysis_object(model_output: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "version": ANALYSIS_VERSION,
        "generatedAt": int(time.time()),
        "sentiment": model_output["sentiment"],
        "confidence": model_output["confidence"],
        "summary": model_output["summary"],
        "impact": model_output["impact"],
        "risks": model_output["risks"],
    }


def select_items_to_analyze(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidates = []
    for item in items:
        url = item.get("url") or ""
        if not url:
            continue

        analysis = item.get("analysis")
        if analysis is None:
            candidates.append(item)
            continue

        version = analysis.get("version")
        if version is None or version < ANALYSIS_VERSION:
            candidates.append(item)

    return candidates[:MAX_ARTICLES_PER_RUN]


def main() -> None:
    items = load_news(NEWS_FILE_PATH)
    client = create_openai_client()

    to_analyze = select_items_to_analyze(items)
    if not to_analyze:
        print("No items to analyze")
        return

    print(f"Analyzing {len(to_analyze)} items")

    for idx, item in enumerate(to_analyze, start=1):
        url = item.get("url")
        print(f"[{idx}/{len(to_analyze)}] id={item.get('id')} url={url}")

        html = fetch_article_html(url) if url else None
        article_text = None
        if html:
            article_text = extract_main_text(html)
            if article_text:
                article_text = truncate_text(article_text, max_chars=6000)

        prompt = build_prompt(item, article_text)
        try:
            model_output = analyze_with_openai(client, prompt)
        except Exception as e:
            print(f"OpenAI error for id={item.get('id')}: {e}")
            continue

        analysis_obj = build_analysis_object(model_output)
        item["analysis"] = analysis_obj

        # 新增：防止 RPM 超限，每条之间停 25 秒
        time.sleep(25)

    save_news(NEWS_FILE_PATH, items)
    print("Done")


if __name__ == "__main__":
    main()
