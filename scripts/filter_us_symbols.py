import os
import sys
from pathlib import Path

import requests


API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
BASE_URL = "https://finnhub.io/api/v1"


def fetch_filtered_us_symbols():
    """
    从 Finnhub 拉 US 交易所股票，并进行筛选：
    - 普通股 / ETF / ADR
    - USD 计价
    - 主板交易所（XNYS, XNAS, ARCX, BATS, IEXG）
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

        if not sym:
            continue

        # 类型过滤：普通股 / ETF / ADR
        # Finnhub 的 type 可能是 "Common Stock", "ETF", "ADR" 等
        if typ not in ("common stock", "etf", "adr"):
            continue

        # 只要 USD
        if currency != "USD":
            continue

        # 主板交易所
        if mic not in ("XNYS", "XNAS", "ARCX", "BATS", "IEXG"):
            continue

        symbols.append(sym)

    # 去重 + 排序
    symbols = sorted(set(symbols))
    print(f"Filtered US tradable symbols: {len(symbols)}")
    return symbols


def write_us_filtered_file(symbols):
    """
    将筛选后的 symbol 写到仓库根目录 us_filtered.txt
    """
    repo_root = Path(__file__).resolve().parent.parent
    out_path = repo_root / "us_filtered.txt"

    content = "\n".join(symbols) + "\n"
    out_path.write_text(content, encoding="utf-8")

    print(f"Written {len(symbols)} symbols to {out_path}")


def main() -> int:
    try:
        symbols = fetch_filtered_us_symbols()
        write_us_filtered_file(symbols)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())