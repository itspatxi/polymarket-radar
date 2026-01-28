import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ====== Config ======
TOP_LEVELS_PER_SIDE = 20         # cuántos niveles guardamos en Silver
DEPTH_BAND = 0.01                # "1 punto" alrededor del midpoint
BUDGETS = [10, 50, 200]          # presupuesto (USDC aprox.) para slippage buy
# ====================


@dataclass
class Level:
    price: float
    size: float


def find_latest_snapshot(base: Path) -> Path:
    """
    Espera estructura: data/bronze/polymarket/YYYY-MM-DD/HHMMSS/...
    Devuelve la carpeta más reciente.
    """
    if not base.exists():
        raise FileNotFoundError(f"No existe {base}. ¿Has ejecutado el Paso 3?")

    # baja dos niveles (fecha/hora)
    candidates = []
    for date_dir in base.iterdir():
        if not date_dir.is_dir():
            continue
        for time_dir in date_dir.iterdir():
            if time_dir.is_dir():
                candidates.append(time_dir)

    if not candidates:
        raise FileNotFoundError(f"No hay snapshots en {base} (esperaba carpetas YYYY-MM-DD/HHMMSS).")

    # orden lexicográfico funciona con YYYY-MM-DD y HHMMSS
    return sorted(candidates)[-1]


def load_orderbooks(snapshot_dir: Path) -> List[Dict[str, Any]]:
    p = snapshot_dir / "books" / "orderbooks.json"
    if not p.exists():
        raise FileNotFoundError(f"No encuentro {p}.")
    return json.loads(p.read_text(encoding="utf-8"))


def parse_levels(side_levels: List[Dict[str, Any]]) -> List[Level]:
    out: List[Level] = []
    for x in side_levels or []:
        try:
            out.append(Level(price=float(x["price"]), size=float(x["size"])))
        except Exception:
            continue
    return out


def best_bid_ask(bids: List[Level], asks: List[Level]) -> Tuple[Optional[float], Optional[float]]:
    bb = max((l.price for l in bids), default=None)
    ba = min((l.price for l in asks), default=None)
    return bb, ba


def midpoint(best_bid: Optional[float], best_ask: Optional[float]) -> Optional[float]:
    if best_bid is None and best_ask is None:
        return None
    if best_bid is None:
        return best_ask
    if best_ask is None:
        return best_bid
    return (best_bid + best_ask) / 2.0


def calc_depth(bids: List[Level], asks: List[Level], mid: Optional[float], band: float) -> Optional[float]:
    if mid is None:
        return None
    lo = mid - band
    hi = mid + band
    bid_depth = sum(l.size for l in bids if l.price >= lo)
    ask_depth = sum(l.size for l in asks if l.price <= hi)
    return bid_depth + ask_depth


def buy_avg_fill_price(asks: List[Level], budget: float) -> Optional[float]:
    """
    Simula compra con presupuesto en $:
      - recorre asks (mejor precio primero)
      - consume shares hasta agotar presupuesto
    Devuelve precio medio pagado ($/share), o None si no puede rellenar nada.
    """
    if budget <= 0 or not asks:
        return None

    asks_sorted = sorted(asks, key=lambda l: l.price)
    remaining = budget
    total_cost = 0.0
    total_shares = 0.0

    for lvl in asks_sorted:
        if lvl.price <= 0:
            continue
        lvl_cost_full = lvl.price * lvl.size
        if remaining >= lvl_cost_full:
            # cogemos todo
            total_cost += lvl_cost_full
            total_shares += lvl.size
            remaining -= lvl_cost_full
        else:
            # cogemos parcial
            partial_shares = remaining / lvl.price
            total_cost += remaining
            total_shares += partial_shares
            remaining = 0.0
            break

        if remaining <= 1e-9:
            break

    if total_shares <= 1e-12:
        return None
    return total_cost / total_shares


def safe_float(x: Optional[float]) -> str:
    return "" if x is None else f"{x:.6f}"


def main():
    bronze_base = Path("data/bronze/polymarket")
    snapshot_dir = find_latest_snapshot(bronze_base)

    # tag para outputs
    tag = snapshot_dir.parent.name + "_" + snapshot_dir.name  # YYYY-MM-DD_HHMMSS
    print(f"[INFO] Usando snapshot: {snapshot_dir}")

    orderbooks = load_orderbooks(snapshot_dir)

    # outputs
    silver_dir = Path("data/silver")
    gold_dir = Path("data/gold")
    silver_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)

    silver_path = silver_dir / f"orderbook_levels_{tag}.csv"
    gold_path = gold_dir / f"metrics_{tag}.csv"

    # SILVER: niveles
    with silver_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["snapshot_ts", "token_id", "side", "level", "price", "size"])

        # GOLD: métricas por token
        gold_rows = []

        for ob in orderbooks:
            token_id = str(ob.get("asset_id") or ob.get("token_id") or "")
            snap_ts = str(ob.get("timestamp") or "")

            bids = parse_levels(ob.get("bids", []))
            asks = parse_levels(ob.get("asks", []))

            # guardar top levels
            bids_sorted = sorted(bids, key=lambda l: l.price, reverse=True)[:TOP_LEVELS_PER_SIDE]
            asks_sorted = sorted(asks, key=lambda l: l.price)[:TOP_LEVELS_PER_SIDE]

            for i, lvl in enumerate(bids_sorted, start=1):
                w.writerow([snap_ts, token_id, "bid", i, lvl.price, lvl.size])
            for i, lvl in enumerate(asks_sorted, start=1):
                w.writerow([snap_ts, token_id, "ask", i, lvl.price, lvl.size])

            bb, ba = best_bid_ask(bids, asks)
            mid = midpoint(bb, ba)
            spread = None if (bb is None or ba is None) else (ba - bb)
            depth = calc_depth(bids, asks, mid, DEPTH_BAND)

            # slippages (buy) por presupuesto
            avg10 = buy_avg_fill_price(asks, 10)
            avg50 = buy_avg_fill_price(asks, 50)
            avg200 = buy_avg_fill_price(asks, 200)

            # baseline: midpoint si existe, si no mejor ask
            baseline = mid if mid is not None else ba
            sl10 = None if (avg10 is None or baseline is None) else (avg10 - baseline)
            sl50 = None if (avg50 is None or baseline is None) else (avg50 - baseline)
            sl200 = None if (avg200 is None or baseline is None) else (avg200 - baseline)

            # score simple para ranking (más alto = mejor)
            # (ajústalo luego en BI/ML; ahora vale para arrancar)
            score = None
            if depth is not None and spread is not None and sl50 is not None:
                score = (depth / 1000.0) - (spread * 3.0) - (sl50 * 4.0)

            gold_rows.append({
                "snapshot_ts": snap_ts,
                "token_id": token_id,
                "best_bid": bb,
                "best_ask": ba,
                "mid": mid,
                "spread": spread,
                "depth_1pt": depth,
                "slippage_buy_10": sl10,
                "slippage_buy_50": sl50,
                "slippage_buy_200": sl200,
                "score": score,
            })

    # escribir GOLD
    with gold_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "snapshot_ts", "token_id",
            "best_bid", "best_ask", "mid", "spread",
            "depth_1pt",
            "slippage_buy_10", "slippage_buy_50", "slippage_buy_200",
            "score"
        ])
        for r in gold_rows:
            w.writerow([
                r["snapshot_ts"], r["token_id"],
                "" if r["best_bid"] is None else r["best_bid"],
                "" if r["best_ask"] is None else r["best_ask"],
                "" if r["mid"] is None else r["mid"],
                "" if r["spread"] is None else r["spread"],
                "" if r["depth_1pt"] is None else r["depth_1pt"],
                "" if r["slippage_buy_10"] is None else r["slippage_buy_10"],
                "" if r["slippage_buy_50"] is None else r["slippage_buy_50"],
                "" if r["slippage_buy_200"] is None else r["slippage_buy_200"],
                "" if r["score"] is None else r["score"],
            ])

    print(f"[OK] Silver CSV: {silver_path}")
    print(f"[OK] Gold  CSV: {gold_path}")
    print("[DONE] Paso 4 completado.")


if __name__ == "__main__":
    main()
