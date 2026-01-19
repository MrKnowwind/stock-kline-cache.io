import os
import sys
import requests

FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
US_FILE_PATH = os.path.join(os.path.dirname(__file__), "..", "us.txt")


def fetch_us_symbols() -> list[str]:
    if not FINNHUB_API_KEY:
        raise RuntimeError("FINNHUB_API_KEY is not set")

    url = "https://finnhub.io/api/v1/stock/symbol"
    params = {
        "exchange": "US",
        "token": FINNHUB_API_KEY,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    symbols: list[str] = []
    for item in data:
        # Finnhub 返回字段：symbol, description 等
        sym = (item.get("symbol") or "").strip()
        if not sym:
            continue
        symbols.append(sym)

    # 去重 + 排序
    symbols = sorted(set(symbols))
    return symbols


def write_us_file(symbols: list[str]) -> None:
    # 一行一个 symbol
    content = "\n".join(symbols) + "\n"
    with open(US_FILE_PATH, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Written {len(symbols)} symbols to {US_FILE_PATH}")


def main() -> int:
    try:
        symbols = fetch_us_symbols()
        write_us_file(symbols)
        return 0
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())