import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

TOP_MARKETS = 100
BOOKS_BATCH = 50
SLEEP_BETWEEN_POSTS = 0.25

OUT_DIR = Path("data/bronze/polymarket_stream")
TOKENS_FILE = OUT_DIR / "tokens_top.json"
MARKETS_FILE = OUT_DIR / "markets_top.json"


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_json_loads(x: Any) -> Any:
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return x
    return x


def fetch_markets(limit: int = 300, offset: int = 0) -> List[Dict[str, Any]]:
    url = f"{GAMMA_BASE}/markets"
    params = {
        "limit": limit,
        "offset": offset,
        "order": "volumeNum",
        "ascending": "false",
        "closed": "false",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError("Respuesta inesperada de Gamma /markets")
    return data


def pick_top_markets(raw_markets: List[Dict[str, Any]], top_n: int) -> List[Dict[str, Any]]:
    cleaned = []
    for m in raw_markets:
        if m.get("enableOrderBook") is not True:
            continue
        if m.get("closed") is True:
            continue
        if m.get("active") is not True:
            continue

        clob_token_ids = safe_json_loads(m.get("clobTokenIds"))
        if isinstance(clob_token_ids, list):
            token_ids = [str(x) for x in clob_token_ids]
        else:
            token_ids = []

        if not token_ids:
            continue

        cleaned.append({
            "market_id": m.get("id"),
            "slug": m.get("slug"),
            "question": m.get("question"),
            "category": m.get("category"),
            "volumeNum": m.get("volumeNum"),
            "liquidityNum": m.get("liquidityNum"),
            "clobTokenIds": token_ids,
        })

    def vol_key(x: Dict[str, Any]) -> float:
        try:
            return float(x.get("volumeNum") or 0.0)
        except Exception:
            return 0.0

    cleaned.sort(key=vol_key, reverse=True)
    return cleaned[:top_n]


def maybe_refresh_tokens(max_age_hours: int = 24) -> List[str]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if TOKENS_FILE.exists():
        age_s = time.time() - TOKENS_FILE.stat().st_mtime
        if age_s < max_age_hours * 3600:
            return json.loads(TOKENS_FILE.read_text(encoding="utf-8"))

    raw = fetch_markets(limit=300, offset=0)
    markets = pick_top_markets(raw, TOP_MARKETS)

    token_ids: List[str] = []
    for m in markets:
        token_ids.extend(m["clobTokenIds"])
    token_ids = sorted(set(token_ids))

    MARKETS_FILE.write_text(json.dumps(markets, ensure_ascii=False, indent=2), encoding="utf-8")
    TOKENS_FILE.write_text(json.dumps(token_ids, ensure_ascii=False, indent=2), encoding="utf-8")

    return token_ids


def chunked(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def fetch_books_bulk(token_ids: List[str]) -> List[Dict[str, Any]]:
    url = f"{CLOB_BASE}/books"
    out: List[Dict[str, Any]] = []
    for batch in chunked(token_ids, BOOKS_BATCH):
        payload = [{"token_id": tid} for tid in batch]
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            out.extend(data)
        else:
            out.append(data)
        time.sleep(SLEEP_BETWEEN_POSTS)
    return out


def append_jsonl(path: Path, obj: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main():
    token_ids = maybe_refresh_tokens(max_age_hours=24)

    books = fetch_books_bulk(token_ids)
    snap = {
        "snapshot_ts_utc": utc_iso(),
        "token_count": len(token_ids),
        "books_count": len(books),
        "books": books,
    }

    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_file = OUT_DIR / f"books_snapshots_{day}.jsonl"
    append_jsonl(out_file, snap)

    print(f"[OK] Snapshot guardado en: {out_file}  (tokens={len(token_ids)}, books={len(books)})")


if __name__ == "__main__":
    main()
