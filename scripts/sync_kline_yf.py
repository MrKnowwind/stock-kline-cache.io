import json
import pathlib
import time
from datetime import date, datetime

import yfinance as yf

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DATA_SYMBOLS = BASE_DIR / "symbols"
DATA_CANDLES_DAILY = BASE_DIR / "candles" / "daily"
DATA_CANDLES_MONTHLY = BASE_DIR / "candles" / "monthly"


def ensure_dirs():
    DATA_SYMBOLS.mkdir(parents=True, exist_ok=True)
    DATA_CANDLES_DAILY.mkdir(parents=True, exist_ok=True)
    DATA_CANDLES_MONTHLY.mkdir(parents=True, exist_ok=True)


def load_symbols():
    path = DATA_SYMBOLS / "us.txt"
    if not path.exists():
        return []
    symbols = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        symbols.append(line)
    return symbols


def save_json(path: pathlib.Path, obj):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(obj, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp.replace(path)


def fetch_daily(symbol: str):
    # 最近 3 年日 K
    print(f"  fetching daily for {symbol}")
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="3y", interval="1d", auto_adjust=False)
    candles = []
    for idx, row in hist.iterrows():
        if any(v is None for v in (row["Open"], row["High"], row["Low"], row["Close"])):
            continue
        d = idx.date() if isinstance(idx, (datetime,)) else idx
        # 转成 epochDay，和你 Android 里的日K编码兼容
        epoch_day = d.toordinal() - date(1970, 1, 1).toordinal()
        candles.append(
            {
                "time": epoch_day,
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
            }
        )
    return candles


def fetch_monthly(symbol: str):
    # 最近 20 年月 K
    print(f"  fetching monthly for {symbol}")
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="20y", interval="1mo", auto_adjust=False)
    candles = []
    for idx, row in hist.iterrows():
        if any(v is None for v in (row["Open"], row["High"], row["Low"], row["Close"])):
            continue
        d = idx.date() if isinstance(idx, (datetime,)) else idx
        encoded = d.year * 100 + d.month  # year*100+month，和你现有月K编码一致
        candles.append(
            {
                "time": encoded,
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
            }
        )
    return candles


def main():
    ensure_dirs()
    symbols = load_symbols()
    if not symbols:
        print("no symbols in symbols/us.txt")
        return

    # 可以通过环境变量限制本次同步的数量
    import os

    limit_str = os.environ.get("SYNC_SYMBOL_LIMIT")
    if limit_str:
        try:
            limit = int(limit_str)
            symbols = symbols[:limit]
        except ValueError:
            pass

    for idx, sym in enumerate(symbols, start=1):
        print(f"[{idx}/{len(symbols)}] syncing {sym}")
        try:
            daily = fetch_daily(sym)
            monthly = fetch_monthly(sym)
        except Exception as e:
            print(f"  error for {sym}: {e}")
            time.sleep(1)
            continue

        save_json(
            DATA_CANDLES_DAILY / f"{sym}.json",
            {"symbol": sym, "interval": "D", "candles": daily},
        )
        save_json(
            DATA_CANDLES_MONTHLY / f"{sym}.json",
            {"symbol": sym, "interval": "M", "candles": monthly},
        )

        # 简单限速，避免短时间内请求太猛
        time.sleep(float(os.environ.get("SYNC_SLEEP_SECONDS", "0.5")))


if __name__ == "__main__":
    main()
