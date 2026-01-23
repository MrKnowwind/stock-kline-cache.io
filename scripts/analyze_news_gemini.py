import json
import os
import time
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup
from readability import Document
import google.generativeai as genai


NEWS_FILE_PATH = os.getenv("NEWS_FILE_PATH", "news/top.json")
MAX_ARTICLES_PER_RUN = int(os.getenv("MAX_ARTICLES_PER_RUN", "50"))
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
ANALYSIS_VERSION = 2


def load_news(path: str) -> List[Dict[str, Any]]:
    print(f"[INFO] Loading news from {path}")
    with open(path, "r", encoding="utf-8") as f:
        items = json.load(f)
    print(f"[INFO] Loaded {len(items)} items from news file")
    return items


def save_news(path: str, items: List[Dict[str, Any]]) -> None:
    print(f"[INFO] Saving news to {path}")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Saved {len(items)} items with analysis back to file")


def fetch_article_html(url: str, timeout: int = 10) -> Optional[str]:
    print(f"[INFO] Fetching article HTML: {url}")
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            )
        }
        resp = requests.get(url, headers=headers, timeout=timeout)
        print(f"[INFO] HTTP status for {url}: {resp.status_code}")
        if resp.status_code != 200:
            return None
        return resp.text
    except Exception as e:
        print(f"[WARN] Failed to fetch article HTML for {url}: {e}")
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
        if text:
            print(f"[INFO] Extracted article text length: {len(text)} chars")
        else:
            print("[INFO] No text extracted from article HTML")
        return text or None
    except Exception as e:
        print(f"[WARN] Failed to extract main text: {e}")
        return None


def truncate_text(text: str, max_chars: int = 6000) -> str:
    if len(text) <= max_chars:
        return text
    print(f"[INFO] Truncating article text from {len(text)} to {max_chars} chars")
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


def configure_gemini() -> None:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")
    genai.configure(api_key=api_key)
    print(f"[INFO] Gemini configured. Model: {GEMINI_MODEL}, analysis version: {ANALYSIS_VERSION}")


def analyze_with_gemini(prompt: str, item_id: Any) -> Dict[str, Any]:
    print(f"[INFO] Calling Gemini for id={item_id}")
    model = genai.GenerativeModel(GEMINI_MODEL)
    try:
        response = model.generate_content(
            prompt,
            generation_config={"response_mime_type": "application/json"},
        )
    except Exception as e:
        msg = str(e)
        print(f"[ERROR] Gemini API call failed for id={item_id}: {msg}")
        if "quota" in msg.lower() or "billing" in msg.lower():
            print("[ERROR] This looks like a quota/billing issue. "
                  "Check your Gemini plan and rate limits.")
        raise

    text = response.text
    print(f"[INFO] Raw Gemini response length for id={item_id}: {len(text)} chars")

    data = json.loads(text)

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

    print(
        f"[INFO] Parsed analysis for id={item_id}: "
        f"sentiment={sentiment}, confidence={confidence}, "
        f"summary_len={len(summary)}, impact_len={len(impact)}, risks_count={len(risks)}"
    )

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

    print(
        f"[INFO] Selected {len(candidates)} items to analyze "
        f"(max per run = {MAX_ARTICLES_PER_RUN})"
    )
    return candidates[:MAX_ARTICLES_PER_RUN]


def main() -> None:
    configure_gemini()
    items = load_news(NEWS_FILE_PATH)

    to_analyze = select_items_to_analyze(items)
    if not to_analyze:
        print("[INFO] No items to analyze")
        return

    print(f"[INFO] Start analyzing {len(to_analyze)} items with Gemini")

    for idx, item in enumerate(to_analyze, start=1):
        item_id = item.get("id")
        url = item.get("url")
        print(f"[INFO] ---- [{idx}/{len(to_analyze)}] id={item_id} url={url} ----")

        html = fetch_article_html(url) if url else None
        article_text = None
        if html:
            article_text = extract_main_text(html)
            if article_text:
                article_text = truncate_text(article_text, max_chars=6000)
        else:
            print(f"[WARN] No HTML fetched for id={item_id}, falling back to summary-only prompt")

        prompt = build_prompt(item, article_text)
        try:
            model_output = analyze_with_gemini(prompt, item_id=item_id)
        except Exception:
            # Already logged inside analyze_with_gemini
            print(f"[ERROR] Skipping id={item_id} due to Gemini error")
            continue

        analysis_obj = build_analysis_object(model_output)
        item["analysis"] = analysis_obj

        time.sleep(1.0)

    save_news(NEWS_FILE_PATH, items)
    print("[INFO] All done")


if __name__ == "__main__":
    main()
