#!/usr/bin/env python3
"""
JAKUU Smart Money Pipeline - Direct Mode

Reads poly_data raw orderFilled.csv directly (skips process_live).
Filters for smart money wallets at scan time, so we never need to
process all 151M trades - just the ones from tracked wallets.

Usage:
  python smart_money_direct.py --poly-data-dir path/to/poly_data --dry-run
  python smart_money_direct.py --poly-data-dir path/to/poly_data
"""

import os
import sys
import math
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import defaultdict

import polars as pl
import requests

D1_DATABASE_ID = os.getenv("D1_DATABASE_ID", "85b521ad-69d2-4713-be3f-ad2e3ffcca6b")
CF_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
CF_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN", "")

SMART_MONEY_WALLETS = {
    "0x9d84ce0306f8551e02efef1680475fc0f1dc1344": "domah",
    "0xd218e474776403a330142299f7796e8ba32eb5c9": "whale_1",
    "0xee613b3fc183ee44f9da9c05f53e2da107e3debf": "whale_2",
    "0x3cf3e8d5427aed066a7a5926980600f6c3cf87b3": "50pence",
    "0x6356fb47642a028bc09df92023c35a21a0b41885": "fhantom",
    "0x7c3db723f1d4d8cb9c550095203b686cb11e5c6b": "car",
    "0x56687bf447db6ffa42ffe2204a05edaa20f55839": "theo4",
}

PLATFORM_WALLETS = {
    "0xc5d563a36ae78145c45a50134d48a1215220f80a",
    "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
}

LOOKBACK_DAYS = 14
MIN_USD_TRADE = 50
MIN_CONVICTION_SCORE = 2.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("smart_money")


class D1Client:
    def __init__(self, account_id, api_token, database_id):
        self.url = "https://api.cloudflare.com/client/v4/accounts/{}/d1/database/{}/query".format(account_id, database_id)
        self.headers = {"Authorization": "Bearer {}".format(api_token), "Content-Type": "application/json"}

    def query(self, sql, params=None):
        body = {"sql": sql}
        if params:
            body["params"] = params
        r = requests.post(self.url, headers=self.headers, json=body, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            raise RuntimeError("D1 error: {}".format(data.get("errors")))
        return data.get("result", [{}])[0]

    def upsert(self, table, row, conflict_col):
        cols = list(row.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        update_cols = [c for c in cols if c != conflict_col]
        set_clause = ", ".join(["{}=excluded.{}".format(c, c) for c in update_cols])
        sql = "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT({}) DO UPDATE SET {}".format(
            table, col_names, placeholders, conflict_col, set_clause
        )
        self.query(sql, [row[c] for c in cols])

    def insert_ignore(self, table, row, unique_col):
        cols = list(row.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        sql = "INSERT OR IGNORE INTO {} ({}) VALUES ({})".format(table, col_names, placeholders)
        self.query(sql, [row[c] for c in cols])


def load_markets(poly_data_dir):
    markets_path = poly_data_dir / "markets.csv"
    log.info("Loading markets from %s...", markets_path)
    df = pl.read_csv(str(markets_path), infer_schema_length=10000, schema_overrides={"token1": pl.Utf8, "token2": pl.Utf8, "condition_id": pl.Utf8})
    log.info("  %d markets loaded", len(df))

    token_map = {}
    for row in df.iter_rows(named=True):
        cid = row.get("condition_id")
        q = row.get("question", "Unknown")
        slug = row.get("market_slug", "")
        ct = row.get("closedTime")
        t1 = str(row.get("token1", ""))
        t2 = str(row.get("token2", ""))
        if t1 and t1 != "None" and t1 != "":
            token_map[t1] = {"condition_id": cid, "question": q, "slug": slug, "side": "token1", "closedTime": ct}
        if t2 and t2 != "None" and t2 != "":
            token_map[t2] = {"condition_id": cid, "question": q, "slug": slug, "side": "token2", "closedTime": ct}
    log.info("  Built token map: %d token IDs", len(token_map))
    return token_map


def scan_smart_money_trades(poly_data_dir, token_map, lookback_days):
    orders_path = poly_data_dir / "goldsky" / "orderFilled.csv"
    if not orders_path.exists():
        log.error("orderFilled.csv not found at %s", orders_path)
        sys.exit(1)

    log.info("Scanning %s for smart money trades...", orders_path)
    log.info("  (This may take a few minutes on a large file)")

    sm_addresses = list(SMART_MONEY_WALLETS.keys())

    lf = pl.scan_csv(str(orders_path), infer_schema_length=10000, schema_overrides={"makerAssetId": pl.Utf8, "takerAssetId": pl.Utf8, "makerAmountFilled": pl.Utf8, "takerAmountFilled": pl.Utf8})
    lf = lf.filter(pl.col("maker").str.to_lowercase().is_in(sm_addresses))
    df = lf.collect(streaming=True)
    log.info("  Found %d trades from smart money wallets", len(df))

    if df.is_empty():
        return []

    if "timestamp" in df.columns:
        df = df.with_columns(
            pl.from_epoch(pl.col("timestamp").cast(pl.Int64), time_unit="s").alias("ts")
        )
        cutoff = datetime.now() - timedelta(days=lookback_days)
        df = df.filter(pl.col("ts") >= cutoff)
        log.info("  After %d-day filter: %d trades", lookback_days, len(df))

    if df.is_empty():
        return []

    trades = []
    unmapped = 0
    for row in df.iter_rows(named=True):
        maker = str(row.get("maker", "")).lower()
        maker_asset = str(row.get("makerAssetId", ""))
        taker_asset = str(row.get("takerAssetId", ""))
        maker_amount = float(row.get("makerAmountFilled", 0)) / 1e6
        taker_amount = float(row.get("takerAmountFilled", 0)) / 1e6

        if maker_asset == "0" or len(maker_asset) < 10:
            direction = "BUY"
            token_id = taker_asset
            usd_amount = maker_amount
            token_amount = taker_amount
            price = maker_amount / taker_amount if taker_amount > 0 else 0
        elif taker_asset == "0" or len(taker_asset) < 10:
            direction = "SELL"
            token_id = maker_asset
            usd_amount = taker_amount
            token_amount = maker_amount
            price = taker_amount / maker_amount if maker_amount > 0 else 0
        else:
            continue

        if usd_amount < MIN_USD_TRADE:
            continue

        market_info = token_map.get(token_id)
        if not market_info:
            unmapped += 1
            continue

        ts_val = row.get("ts")
        ts_str = ts_val.isoformat() if ts_val else None

        trades.append({
            "wallet_address": maker,
            "wallet_alias": SMART_MONEY_WALLETS.get(maker, "unknown"),
            "condition_id": market_info["condition_id"],
            "market_question": market_info["question"],
            "market_slug": market_info["slug"],
            "direction": direction,
            "token_side": market_info["side"],
            "price": round(price, 6),
            "usd_amount": round(usd_amount, 2),
            "token_amount": round(token_amount, 2),
            "trade_timestamp": ts_str,
            "transaction_hash": str(row.get("transactionHash", "")),
            "closed_time": market_info.get("closedTime"),
        })

    if unmapped > 0:
        log.info("  %d trades could not be mapped to markets", unmapped)
    log.info("  %d mappable smart money trades in lookback window", len(trades))
    return trades


def compute_signals(trades):
    if not trades:
        return []

    by_market = defaultdict(list)
    for t in trades:
        by_market[t["condition_id"]].append(t)

    signals = []
    for cid, market_trades in by_market.items():
        question = market_trades[0]["market_question"]
        slug = market_trades[0]["market_slug"]
        closed_time = market_trades[0].get("closed_time")

        buy_usd = sum(t["usd_amount"] for t in market_trades if t["direction"] == "BUY")
        sell_usd = sum(t["usd_amount"] for t in market_trades if t["direction"] == "SELL")
        total_usd = buy_usd + sell_usd

        if total_usd == 0:
            continue

        if buy_usd > sell_usd * 1.5:
            net_dir = "YES"
        elif sell_usd > buy_usd * 1.5:
            net_dir = "NO"
        else:
            net_dir = "MIXED"

        dominant_pct = max(buy_usd, sell_usd) / total_usd * 100
        unique_wallets = len(set(t["wallet_address"] for t in market_trades))
        avg_price = sum(t["price"] for t in market_trades) / len(market_trades)

        timestamps = [t["trade_timestamp"] for t in market_trades if t["trade_timestamp"]]
        earliest = min(timestamps) if timestamps else None
        latest = max(timestamps) if timestamps else None

        now = datetime.now(timezone.utc)
        signal_age = 999
        if latest:
            try:
                latest_dt = datetime.fromisoformat(latest)
                if latest_dt.tzinfo is None:
                    latest_dt = latest_dt.replace(tzinfo=timezone.utc)
                signal_age = (now - latest_dt).total_seconds() / 3600
            except Exception:
                signal_age = 999

        size_score = min(3.0, max(0, math.log10(max(total_usd, 1)) - 2.0))
        agreement_score = max(0, (dominant_pct - 50) / 50 * 3)
        if signal_age < 6:
            recency_score = 2.0
        elif signal_age < 24:
            recency_score = 1.5
        elif signal_age < 48:
            recency_score = 1.0
        elif signal_age < 168:
            recency_score = 0.5
        else:
            recency_score = 0.0
        breadth_score = min(2.0, unique_wallets * 0.5 + 0.5) if unique_wallets > 0 else 0
        conviction = round(size_score + agreement_score + recency_score + breadth_score, 1)

        signals.append({
            "condition_id": str(cid),
            "market_question": str(question)[:500],
            "market_slug": str(slug) if slug else None,
            "net_direction": net_dir,
            "total_smart_usd": round(total_usd, 2),
            "smart_wallet_count": unique_wallets,
            "yes_usd": round(buy_usd, 2),
            "no_usd": round(sell_usd, 2),
            "avg_entry_price": round(avg_price, 4),
            "dominant_pct": round(dominant_pct, 1),
            "earliest_trade": earliest,
            "latest_trade": latest,
            "signal_age_hours": round(signal_age, 1),
            "conviction_score": conviction,
            "market_end_date": str(closed_time) if closed_time else None,
            "computed_at": now.isoformat(),
        })

    signals.sort(key=lambda s: s["conviction_score"], reverse=True)
    log.info("  Computed signals for %d markets", len(signals))
    return signals


def run(poly_data_dir, dry_run=False, lookback_days=LOOKBACK_DAYS):
    log.info("=" * 60)
    log.info("JAKUU Smart Money Pipeline - Direct Mode")
    log.info("=" * 60)

    token_map = load_markets(poly_data_dir)
    trades = scan_smart_money_trades(poly_data_dir, token_map, lookback_days)

    if not trades:
        log.warning("No smart money trades found. Exiting.")
        return

    signals = compute_signals(trades)
    filtered = [s for s in signals if s["conviction_score"] >= MIN_CONVICTION_SCORE]

    log.info("")
    log.info("=" * 60)
    log.info("Results: %d markets with SM activity, %d above threshold", len(signals), len(filtered))
    log.info("=" * 60)
    for s in filtered[:15]:
        log.info("  [%4.1f] %5s $%10.0f | %dw | %.0fh ago | %s",
            s["conviction_score"], s["net_direction"], s["total_smart_usd"],
            s["smart_wallet_count"], s["signal_age_hours"], s["market_question"][:55]
        )

    if dry_run:
        log.info("")
        log.info("[DRY RUN] Would sync %d signals and %d trades to D1", len(filtered), len(trades))
        return

    if not CF_ACCOUNT_ID or not CF_API_TOKEN:
        log.error("Set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN env vars, or use --dry-run")
        sys.exit(1)

    d1 = D1Client(CF_ACCOUNT_ID, CF_API_TOKEN, D1_DATABASE_ID)

    log.info("")
    log.info("Syncing %d signals to D1...", len(filtered))
    synced = 0
    for s in filtered:
        try:
            d1.upsert("smart_money_signals", s, "condition_id")
            synced += 1
            if synced % 25 == 0:
                log.info("  %d/%d signals synced", synced, len(filtered))
                time.sleep(0.5)
        except Exception as e:
            log.warning("  Failed: %s", e)
    log.info("  Done: %d signals synced", synced)

    log.info("")
    log.info("Syncing trades to D1 (high-conviction markets only)...")
    high_cids = set(s["condition_id"] for s in filtered)
    trade_synced = 0
    for t in trades:
        if t["condition_id"] not in high_cids:
            continue
        row = {
            "wallet_address": t["wallet_address"],
            "condition_id": t["condition_id"],
            "market_question": t["market_question"][:500],
            "direction": t["direction"],
            "price": t["price"],
            "usd_amount": t["usd_amount"],
            "token_amount": t["token_amount"],
            "trade_timestamp": t["trade_timestamp"],
            "transaction_hash": t["transaction_hash"],
        }
        try:
            d1.insert_ignore("smart_money_trades", row, "transaction_hash")
            trade_synced += 1
        except Exception:
            pass
    log.info("  Done: %d trades synced", trade_synced)

    log.info("")
    log.info("=" * 60)
    log.info("Pipeline complete!")
    log.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="JAKUU Smart Money Pipeline")
    parser.add_argument("--poly-data-dir", type=Path, default=Path.home() / "poly_data")
    parser.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(args.poly_data_dir, args.dry_run, args.lookback_days)
