import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


GAMMA_BASE = "https://gamma-api.polymarket.com"   # Gamma Markets API :contentReference[oaicite:1]{index=1}
CLOB_BASE = "https://clob.polymarket.com"         # CLOB API :contentReference[oaicite:2]{index=2}


def utc_now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d/%H%M%S")


def chunked(items: List[Any], size: int) -> List[List[Any]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def safe_json_loads(x: Any) -> Any:
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return x
    return x


def fetch_markets(limit: int = 200, offset: int = 0) -> List[Dict[str, Any]]:
    """
    Gamma: GET /markets (soporta limit/offset/order/ascending/closed, etc.). :contentReference[oaicite:3]{index=3}
    """
    url = f"{GAMMA_BASE}/markets"
    params = {
        "limit": limit,
        "offset": offset,
        "order": "volumeNum",   # ordenar por volumen numérico
        "ascending": "false",
        "closed": "false",      # query param documentado :contentReference[oaicite:4]{index=4}
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError("Respuesta inesperada de Gamma /markets (se esperaba lista).")
    return data


def pick_top_markets(raw_markets: List[Dict[str, Any]], top_n: int = 100) -> List[Dict[str, Any]]:
    """
    Filtra y normaliza mercados que tengan enableOrderBook=true (CLOB-tradeable). :contentReference[oaicite:5]{index=5}
    """
    cleaned: List[Dict[str, Any]] = []
    for m in raw_markets:
        if not m.get("enableOrderBook", False):
            continue
        if m.get("closed") is True:
            continue
        # "active" a veces puede venir null; lo tratamos como False si no es True
        if m.get("active") is not True:
            continue

        clob_token_ids = safe_json_loads(m.get("clobTokenIds"))
        if isinstance(clob_token_ids, str):
            # a veces viene como string no parseable; intenta separar manualmente
            # (lo dejamos como lista vacía si no es usable)
            clob_token_ids_list: List[str] = []
        elif isinstance(clob_token_ids, list):
            clob_token_ids_list = [str(x) for x in clob_token_ids]
        else:
            clob_token_ids_list = []

        if len(clob_token_ids_list) == 0:
            continue

        cleaned.append({
            "market_id": m.get("id"),
            "slug": m.get("slug"),
            "question": m.get("question"),
            "category": m.get("category"),
            "startDate": m.get("startDate"),
            "endDate": m.get("endDate"),
            "active": m.get("active"),
            "closed": m.get("closed"),
            "enableOrderBook": m.get("enableOrderBook"),
            "volumeNum": m.get("volumeNum"),
            "liquidityNum": m.get("liquidityNum"),
            "clobTokenIds": clob_token_ids_list,
        })

    # Ordena por volumeNum si existe
    def vol_key(x: Dict[str, Any]) -> float:
        try:
            return float(x.get("volumeNum") or 0.0)
        except Exception:
            return 0.0

    cleaned.sort(key=vol_key, reverse=True)
    return cleaned[:top_n]


def fetch_orderbooks_bulk(token_ids: List[str], batch_size: int = 50, sleep_s: float = 0.2) -> List[Dict[str, Any]]:
    """
    CLOB: POST /books (varios token_id por request). :contentReference[oaicite:6]{index=6}
    """
    url = f"{CLOB_BASE}/books"
    out: List[Dict[str, Any]] = []
    for batch in chunked(token_ids, batch_size):
        payload = [{"token_id": tid} for tid in batch]
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            out.extend(data)
        else:
            # por si viene como objeto
            out.append(data)
        time.sleep(sleep_s)
    return out


def fetch_price_history(token_id: str, interval: str = "1w", fidelity_min: int = 15) -> Dict[str, Any]:
    """
    CLOB: GET /prices-history con params market=<token_id>, interval, fidelity. :contentReference[oaicite:7]{index=7}
    """
    url = f"{CLOB_BASE}/prices-history"
    params = {
        "market": token_id,      # nombre del parámetro en docs :contentReference[oaicite:8]{index=8}
        "interval": interval,
        "fidelity": fidelity_min,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    TOP_MARKETS = 100
    HISTORY_TOP_MARKETS = 20      # para no bajar demasiado al inicio
    HISTORY_INTERVAL = "1w"
    HISTORY_FIDELITY_MIN = 15

    tag = utc_now_tag()

    base = Path("data/bronze/polymarket") / tag
    (base / "markets").mkdir(parents=True, exist_ok=True)
    (base / "books").mkdir(parents=True, exist_ok=True)
    (base / "prices_history").mkdir(parents=True, exist_ok=True)

    # 1) Markets (Gamma)
    raw = fetch_markets(limit=300, offset=0)
    markets = pick_top_markets(raw, top_n=TOP_MARKETS)

    (base / "markets" / "markets_top.json").write_text(json.dumps(markets, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] Markets guardados: {base / 'markets' / 'markets_top.json'}  (n={len(markets)})")

    # 2) Token IDs
    token_ids: List[str] = []
    for m in markets:
        token_ids.extend(m["clobTokenIds"])
    token_ids = sorted(set(token_ids))
    print(f"[INFO] Tokens únicos: {len(token_ids)}")

    # 3) Orderbooks bulk (CLOB)
    books = fetch_orderbooks_bulk(token_ids, batch_size=50)
    (base / "books" / "orderbooks.json").write_text(json.dumps(books, ensure_ascii=False), encoding="utf-8")
    print(f"[OK] Orderbooks guardados: {base / 'books' / 'orderbooks.json'}  (n={len(books)})")

    # 4) Price history (solo top 20 mercados para arrancar)
    hist_tokens: List[str] = []
    for m in markets[:HISTORY_TOP_MARKETS]:
        hist_tokens.extend(m["clobTokenIds"])
    hist_tokens = sorted(set(hist_tokens))

    for tid in hist_tokens:
        try:
            h = fetch_price_history(tid, interval=HISTORY_INTERVAL, fidelity_min=HISTORY_FIDELITY_MIN)
            (base / "prices_history" / f"{tid}.json").write_text(json.dumps(h, ensure_ascii=False), encoding="utf-8")
            time.sleep(0.15)
        except Exception as e:
            print(f"[WARN] prices-history falló para token {tid}: {e}")

    print(f"[OK] Price history guardado en: {base / 'prices_history'}  (tokens={len(hist_tokens)})")
    print("[DONE] Snapshot completado.")


if __name__ == "__main__":
    main()
