import json
import math
from pathlib import Path
from typing import Dict, List, Iterable, Tuple, Optional

import yfinance as yf
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
SYMBOLS_FILE = BASE_DIR / "symbols" / "us.txt"
OUTPUT_FILE = BASE_DIR / "quotes" / "latest.json"

CHUNK_SIZE = 300


def load_symbols() -> List[str]:
    symbols: List[str] = []
    seen = set()
    if not SYMBOLS_FILE.exists():
        raise FileNotFoundError(f"symbols file not found: {SYMBOLS_FILE}")
    with SYMBOLS_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s not in seen:
                seen.add(s)
                symbols.append(s)
    return symbols


def chunked(seq: List[str], size: int) -> Iterable[List[str]]:
    if size <= 0:
        size = len(seq)
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def extract_ohlc(
    df: pd.DataFrame,
) -> Optional[Tuple[float, float, float, float, Optional[float]]]:
    clean = df.dropna(how="all")
    if clean.empty:
        return None
    last = clean.iloc[-1]
    try:
        c = float(last["Close"])
        o = float(last["Open"])
        h = float(last["High"])
        l = float(last["Low"])
    except Exception:
        return None
    pc: Optional[float] = None
    if len(clean) >= 2:
        prev = clean.iloc[-2]
        try:
            pc = float(prev["Close"])
        except Exception:
            pc = None
    return c, o, h, l, pc


def compute_change(
    c: float, pc: Optional[float]
) -> Tuple[Optional[float], Optional[float]]:
    if pc is None or not math.isfinite(pc) or pc == 0.0:
        return None, None
    d = c - pc
    dp = d / pc * 100.0
    return d, dp


def format_quote(
    c: float,
    o: float,
    h: float,
    l: float,
    pc: Optional[float],
) -> Dict[str, Optional[float]]:
    d, dp = compute_change(c, pc)
    return {
        "c": float(f"{c:.4f}"),
        "d": float(f"{d:.4f}") if d is not None else None,
        "dp": float(f"{dp:.4f}") if dp is not None else None,
        "h": float(f"{h:.4f}"),
        "l": float(f"{l:.4f}"),
        "o": float(f"{o:.4f}"),
        "pc": float(f"{pc:.4f}") if pc is not None else None,
    }


def fetch_single_quote(symbol: str) -> Optional[Dict[str, Optional[float]]]:
    try:
        df = yf.download(
            symbol,
            period="2d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as e:
        print(f"single failed for {symbol}: {e}")
        return None
    ohlc = extract_ohlc(df)
    if ohlc is None:
        return None
    c, o, h, l, pc = ohlc
    return format_quote(c, o, h, l, pc)


def update_quotes_batch(
    symbols: List[str],
) -> Dict[str, Dict[str, Optional[float]]]:
    if not symbols:
        return {}
    try:
        data = yf.download(
            symbols,
            period="2d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
    except Exception as e:
        print(f"batch download failed ({len(symbols)} symbols): {e}")
        data = pd.DataFrame()
    result: Dict[str, Dict[str, Optional[float]]] = {}
    missing: List[str] = []
    if isinstance(data.columns, pd.MultiIndex):
        for sym in symbols:
            if sym not in data.columns.levels[0]:
                missing.append(sym)
                continue
            df = data[sym]
            ohlc = extract_ohlc(df)
            if ohlc is None:
                missing.append(sym)
                continue
            c, o, h, l, pc = ohlc
            result[sym] = format_quote(c, o, h, l, pc)
    else:
        ohlc = extract_ohlc(data)
        if ohlc is None:
            missing.extend(symbols)
        else:
            c, o, h, l, pc = ohlc
            quote = format_quote(c, o, h, l, pc)
            for sym in symbols:
                result[sym] = quote
    if missing:
        print(f"missing from batch: {len(missing)} symbols, fetching individually")
        for sym in missing:
            q = fetch_single_quote(sym)
            if q is not None:
                result[sym] = q
    return result


def fetch_all_quotes(symbols: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    all_quotes: Dict[str, Dict[str, Optional[float]]] = {}
    for batch in chunked(symbols, CHUNK_SIZE):
        try:
            q = update_quotes_batch(batch)
            all_quotes.update(q)
        except Exception as e:
            print(f"batch failed ({len(batch)} symbols): {e}")
    return all_quotes


def write_json_atomic(path: Path, data: Dict[str, Dict[str, Optional[float]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    tmp.replace(path)


def main() -> None:
    symbols = load_symbols()
    print(f"total symbols: {len(symbols)}")
    quotes = fetch_all_quotes(symbols)
    print(f"got quotes for {len(quotes)} symbols")
    write_json_atomic(OUTPUT_FILE, quotes)
    print(f"written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
