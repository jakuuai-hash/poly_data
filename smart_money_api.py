#!/usr/bin/env python3
"""
JAKUU Smart Money Pipeline - API Mode (S86)

Queries the Polymarket Data API directly for each tracked wallet's trade
activity. Eliminates the 37GB CSV download entirely.

Same conviction scoring as smart_money_direct.py but runs in ~30 seconds
instead of ~60 minutes.

Usage:
  python smart_money_api.py --dry-run        # preview, no D1 writes
  python smart_money_api.py                  # full run, pushes to D1
"""

import os
import sys
import math
import time
import json
import logging
import argparse
from datetime import datetime, timedelta, timezone
from collections import defaultdict

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

LOOKBACK_DAYS = 14
MIN_USD_TRADE = 50
MIN_CONVICTION_SCORE = 2.0
DATA_API_BASE = "https://data-api.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("smart_money_api")


class D1Client:
    """Cloudflare D1 REST API client."""
    def __init__(self, account_id, api_token, database_id):
        self.url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
        self.headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}

    def query(self, sql, params=None):
        body = {"sql": sql}
        if params:
            body["params"] = params
        r = requests.post(self.url, headers=self.headers, json=body, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            raise RuntimeError(f"D1 error: {data.get('errors')}")
        return data.get("result", [{}])[0]

    def upsert(self, table, row, conflict_col):
        cols = list(row.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        update_cols = [c for c in cols if c != conflict_col]
        set_clause = ", ".join([f"{c}=excluded.{c}" for c in update_cols])
        sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT({conflict_col}) DO UPDATE SET {set_clause}"
        self.query(sql, [row[c] for c in cols])

    def insert_ignore(self, table, row, unique_col):
        cols = list(row.keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_names = ", ".join(cols)
        sql = f"INSERT OR IGNORE INTO {table} ({col_names}) VALUES ({placeholders})"
        self.query(sql, [row[c] for c in cols])


def fetch_wallet_trades(address, alias, lookback_days):
    """Fetch trade activity from PM Data API for a single wallet."""
    trades = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    cursor = None
    page = 0

    while True:
        page += 1
        params = {
            "user": address,
            "type": "trade",
            "limit": 500,
        }
        if cursor:
            params["cursor"] = cursor

        try:
            r = requests.get(f"{DATA_API_BASE}/activity", params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning("  API error for %s (page %d): %s", alias, page, e)
            break

        # Handle both list and dict response formats
        items = data if isinstance(data, list) else data.get("data", data.get("activity", []))
        if not items:
            break

        for item in items:
            try:
                # Parse timestamp — API returns ISO format or epoch
                ts_raw = item.get("timestamp") or item.get("createdAt") or item.get("created_at")
                if ts_raw is None:
                    continue
                if isinstance(ts_raw, (int, float)):
                    ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
                else:
                    ts = datetime.fromisoformat(str(ts_raw).replace("Z", "+00:00"))
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)

                if ts < cutoff:
                    # Past lookback window — stop paginating
                    return trades

                # Extract trade fields — PM Data API format
                condition_id = item.get("conditionId") or item.get("condition_id") or ""
                question = item.get("title") or item.get("question") or item.get("market", {}).get("question", "")
                slug = item.get("slug") or item.get("market", {}).get("slug", "")

                side = str(item.get("side") or item.get("type") or "").upper()
                if side not in ("BUY", "SELL"):
                    # Try to infer from other fields
                    action = str(item.get("action", "")).upper()
                    if "BUY" in action:
                        side = "BUY"
                    elif "SELL" in action:
                        side = "SELL"
                    else:
                        continue

                # Amount in USD
                usd = float(item.get("usdcSize") or item.get("size") or item.get("amount") or 0)
                if usd < MIN_USD_TRADE:
                    continue

                price = float(item.get("price") or item.get("avgPrice") or 0)
                token_amount = usd / price if price > 0 else 0
                tx_hash = item.get("transactionHash") or item.get("txHash") or item.get("proxyTxnHash") or ""
                outcome = item.get("outcome") or item.get("token_outcome") or ""
                closed_time = item.get("endDate") or item.get("market", {}).get("endDate", "")

                trades.append({
                    "wallet_address": address.lower(),
                    "wallet_alias": alias,
                    "condition_id": str(condition_id),
                    "market_question": str(question)[:500],
                    "market_slug": str(slug) if slug else None,
                    "direction": side,
                    "token_side": outcome,
                    "price": round(price, 6),
                    "usd_amount": round(usd, 2),
                    "token_amount": round(token_amount, 2),
                    "trade_timestamp": ts.isoformat(),
                    "transaction_hash": str(tx_hash),
                    "closed_time": str(closed_time) if closed_time else None,
                })
            except Exception as e:
                log.debug("  Skipping malformed trade: %s", e)
                continue

        # Pagination — check for next cursor
        if isinstance(data, dict):
            cursor = data.get("next_cursor") or data.get("nextCursor")
            if not cursor:
                break
        else:
            # List response — if we got a full page, there might be more
            if len(items) < 500:
                break
            # Use the last item's timestamp as a pseudo-cursor
            # (but the API might not support this — stop after one page for safety)
            break

        time.sleep(0.3)  # rate limit courtesy

    return trades


def compute_signals(trades):
    """Compute conviction signals per market. Same algorithm as smart_money_direct.py."""
    if not trades:
        return []

    by_market = defaultdict(list)
    for t in trades:
        if t["condition_id"]:
            by_market[t["condition_id"]].append(t)

    signals = []
    for cid, market_trades in by_market.items():
        question = market_trades[0]["market_question"]
        slug = market_trades[0].get("market_slug")
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

        # Conviction score: same formula as smart_money_direct.py
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


def run(dry_run=False, lookback_days=LOOKBACK_DAYS):
    log.info("=" * 60)
    log.info("JAKUU Smart Money Pipeline - API Mode (S86)")
    log.info("=" * 60)
    log.info("Lookback: %d days | Min trade: $%d | Min conviction: %.1f",
             lookback_days, MIN_USD_TRADE, MIN_CONVICTION_SCORE)

    # Fetch trades from PM Data API for each wallet
    all_trades = []
    for address, alias in SMART_MONEY_WALLETS.items():
        log.info("Fetching trades for %s (%s)...", alias, address[:10])
        wallet_trades = fetch_wallet_trades(address, alias, lookback_days)
        log.info("  -> %d trades in lookback window", len(wallet_trades))
        all_trades.extend(wallet_trades)
        time.sleep(0.5)  # rate limit courtesy between wallets

    log.info("")
    log.info("Total trades from %d wallets: %d", len(SMART_MONEY_WALLETS), len(all_trades))

    if not all_trades:
        log.warning("No smart money trades found. Exiting.")
        return

    # Compute conviction signals
    signals = compute_signals(all_trades)
    filtered = [s for s in signals if s["conviction_score"] >= MIN_CONVICTION_SCORE]

    log.info("")
    log.info("=" * 60)
    log.info("Results: %d markets with SM activity, %d above threshold", len(signals), len(filtered))
    log.info("=" * 60)
    for s in filtered[:15]:
        log.info("  [%4.1f] %5s $%10.0f | %dw | %.0fh ago | %s",
                 s["conviction_score"], s["net_direction"], s["total_smart_usd"],
                 s["smart_wallet_count"], s["signal_age_hours"], s["market_question"][:55])

    if dry_run:
        log.info("")
        log.info("[DRY RUN] Would sync %d signals and %d trades to D1", len(filtered), len(all_trades))
        return

    if not CF_ACCOUNT_ID or not CF_API_TOKEN:
        log.error("Set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN env vars, or use --dry-run")
        sys.exit(1)

    d1 = D1Client(CF_ACCOUNT_ID, CF_API_TOKEN, D1_DATABASE_ID)

    # Sync signals to D1
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
            log.warning("  Signal sync failed: %s", e)
    log.info("  Done: %d signals synced", synced)

    # Sync individual trades (high-conviction markets only)
    log.info("")
    log.info("Syncing trades to D1 (high-conviction markets only)...")
    high_cids = set(s["condition_id"] for s in filtered)
    trade_synced = 0
    for t in all_trades:
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
    parser = argparse.ArgumentParser(description="JAKUU Smart Money Pipeline - API Mode")
    parser.add_argument("--lookback-days", type=int, default=LOOKBACK_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(args.dry_run, args.lookback_days)
