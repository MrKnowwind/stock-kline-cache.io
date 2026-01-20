import os
import sys
import time
from pathlib import Path

import requests


API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
BASE_URL = "https://finnhub.io/api/v1"


def get_us_symbols():
    """
    从 Finnhub 拉 US 交易所的全部股票列表，过滤出常规美股。
    """
    if not API_KEY:
        raise RuntimeError("FINNHUB_API_KEY is not set")

    url = f"{BASE_URL}/stock/symbol"
    params = {
        "exchange": "US",
        "token": API_KEY,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    symbols = []
    for item in data:
        sym = (item.get("symbol") or "").strip()
        typ = (item.get("type") or "").strip().lower()
        mic = (item.get("mic") or "").strip().upper()
        currency = (item.get("currency") or "").strip().upper()

        # 只要普通股 + USD + 主板交易所（简单过滤，避免 2.9 万个全要）
        if not sym:
            continue
        if typ not in ("common stock", "etf", "adr"):
            continue
        if currency != "USD":
            continue
        if mic not in ("XNYS", "XNAS", "ARCX", "BATS", "IEXG"):
            continue

        symbols.append(sym)

    # 去重排序（按字母只是稳定，不代表热门）
    symbols = sorted(set(symbols))
    print(f"Filtered US tradable symbols: {len(symbols)}")
    return symbols


def get_market_cap(symbol: str) -> float:
    """
    调用 Finnhub /stock/metric 获取市值（marketCapitalization）。
    """
    url = f"{BASE_URL}/stock/metric"
    params = {
        "symbol": symbol,
        "metric": "all",
        "token": API_KEY,
    }
    resp = requests.get(url, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json() or {}
    metric = data.get("metric") or {}
    cap = metric.get("marketCapitalization")
    if cap is None:
        return 0.0
    return float(cap)


def select_top_symbols(limit: int = 2000):
    symbols = get_us_symbols()

    # 为了控制请求量，这里最多只处理前 5000 个符号
    symbols = symbols[:5000]
    print(f"Will fetch metrics for at most {len(symbols)} symbols")

    results = []
    for i, sym in enumerate(symbols, start=1):
        try:
            cap = get_market_cap(sym)
        except Exception as e:
            print(f"[{i}/{len(symbols)}] {sym}: error {e}", file=sys.stderr)
            cap = 0.0

        results.append((sym, cap))
        if i % 50 == 0:
            print(f"[{i}/{len(symbols)}] processed")

        # 简单限速，避免触发免费额度限制
        time.sleep(1)

    # 按市值从大到小排序
    results.sort(key=lambda x: x[1], reverse=True)

    top = results[:limit]
    return top


def write_top_file(top_symbols):
    repo_root = Path(__file__).resolve().parent.parent
    out_path = repo_root / "us_top2000.txt"
    lines = [s for (s, cap) in top_symbols if s]
    content = "\n".join(lines) + "\n"
    out_path.write_text(content, encoding="utf-8")
    print(f"Written {len(lines)} symbols to {out_path}")


def main() -> int:
    try:
        top = select_top_symbols(limit=2000)
        write_top_file(top)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())