#!/usr/bin/env python3
"""
JAKUU Smart-Money Lifecycle Dump — S145.2

Pulls /activity history for every wallet in smart_money_wallets,
pairs entries to exits via FIFO, joins with Gamma market endDate,
and upserts complete lifecycles into smart_money_lifecycles.

S145.2 changes vs S145.1:
  (1) Gamma metadata: per-conditionId loop instead of broken batch query.
      The batch path (?conditionIds=A&conditionIds=B&...) returns at most
      one match — verified empirically (1/104 markets resolved on run #91).
  (2) Wallet priority ordering: mirror_copy first, then oracle_signal,
      then alpha within tier. Ensures Erasmus dumps before whales when
      timeout pressure exists.
  (3) MAX_RECORDS_PER_WALLET cap (15000) preserved from S145.1.
  (4) Per-wallet streaming upsert preserved — partial saves on timeout.
"""

import os, sys, time, hashlib, logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque
import requests

D1_DATABASE_ID = os.getenv("D1_DATABASE_ID", "85b521ad-69d2-4713-be3f-ad2e3ffcca6b")
CF_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
CF_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN", "")

DATA_API_BASE = "https://data-api.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

LOOKBACK_DAYS = 60
PAGE_SIZE = 500
MAX_RECORDS_PER_WALLET = 15000
MIN_USD_TRADE = 1.0
REQUEST_PAUSE_S = 0.3
GAMMA_PAUSE_S = 0.15

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dump_lifecycles")


class D1Client:
    def __init__(self, account_id, api_token, database_id):
        self.url = (
            f"https://api.cloudflare.com/client/v4/accounts/"
            f"{account_id}/d1/database/{database_id}/query"
        )
        self.headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        }

    def query(self, sql, params=None):
        body = {"sql": sql}
        if params is not None:
            body["params"] = params
        r = requests.post(self.url, headers=self.headers, json=body, timeout=60)
        r.raise_for_status()
        data = r.json()
        if not data.get("success"):
            raise RuntimeError(f"D1 error: {data.get('errors')}")
        return data.get("result", [{}])[0]


def parse_ts(ts_raw):
    if ts_raw is None or ts_raw == "":
        return None
    if isinstance(ts_raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(ts_raw), tz=timezone.utc)
        except (ValueError, OSError):
            return None
    s = str(ts_raw).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def round_or_none(v, ndigits):
    return round(v, ndigits) if v is not None else None


def fetch_registry_wallets(d1):
    """
    Priority order: mirror_copy first (signals being copied to live trades),
    then oracle_signal (gating Claude calls), then everything else. Within
    each tier, alias asc for determinism.
    """
    res = d1.query(
        "SELECT address, alias, engine_target, tier "
        "FROM smart_money_wallets "
        "WHERE status = 'active' "
        "ORDER BY "
        "  CASE engine_target "
        "    WHEN 'mirror_copy'   THEN 0 "
        "    WHEN 'oracle_signal' THEN 1 "
        "    ELSE 2 "
        "  END, "
        "  CASE tier "
        "    WHEN 'sharp'   THEN 0 "
        "    WHEN 'whale'   THEN 1 "
        "    WHEN 'tracked' THEN 2 "
        "    ELSE 3 "
        "  END, "
        "  alias ASC"
    )
    return [
        (row["address"].lower(), row.get("alias") or row["address"][:10])
        for row in (res.get("results") or [])
    ]


def fetch_wallet_activity(address):
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    cutoff_epoch = int(cutoff.timestamp())
    all_records = []
    seen_tx = set()
    end_param = None
    page = 0

    while True:
        page += 1
        params = {
            "user": address,
            "type": "TRADE,REDEEM",
            "limit": PAGE_SIZE,
            "sortBy": "TIMESTAMP",
            "sortDirection": "DESC",
        }
        if end_param is not None:
            params["end"] = end_param

        try:
            r = requests.get(f"{DATA_API_BASE}/activity", params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning("  /activity error p%d: %s", page, e)
            break

        items = data if isinstance(data, list) else (data.get("data") or [])
        if not items:
            break

        new_count = 0
        earliest_ts = None
        for item in items:
            tx = item.get("transactionHash") or item.get("txHash")
            if not tx or tx in seen_tx:
                continue
            ts = parse_ts(item.get("timestamp"))
            if ts is None:
                continue
            if ts < cutoff:
                return all_records
            seen_tx.add(tx)
            item["_parsed_ts"] = ts
            all_records.append(item)
            new_count += 1
            if earliest_ts is None or ts < earliest_ts:
                earliest_ts = ts

        if len(all_records) >= MAX_RECORDS_PER_WALLET:
            log.warning(
                "  HIT MAX_RECORDS cap (%d) — likely market-maker, skipping rest",
                MAX_RECORDS_PER_WALLET,
            )
            return all_records

        if len(items) < PAGE_SIZE or new_count == 0 or earliest_ts is None:
            break
        next_end = int(earliest_ts.timestamp()) - 1
        if next_end < cutoff_epoch:
            break
        end_param = next_end
        time.sleep(REQUEST_PAUSE_S)

    return all_records


def fetch_market_metadata_one(condition_id):
    """
    Per-id Gamma lookup. The batch path (?conditionIds=A&conditionIds=B)
    returns ≤1 result when conditionIds is repeated — broken in S145.1.
    Per-id is slower but correct.
    """
    try:
        r = requests.get(
            f"{GAMMA_API_BASE}/markets",
            params={"conditionIds": condition_id, "limit": 1},
            timeout=15,
        )
        r.raise_for_status()
        items = r.json() or []
        if not items:
            return None
        m = items[0] if isinstance(items, list) else items
        return {
            "end_date": m.get("endDate") or m.get("end_date_iso"),
            "question": m.get("question"),
        }
    except Exception as e:
        log.debug("  Gamma miss for %s: %s", condition_id[:10], e)
        return None


def fetch_markets_metadata(condition_ids):
    out = {}
    cid_list = sorted({c.lower() for c in condition_ids if c})
    if not cid_list:
        return out
    log.info("  Gamma metadata for %d markets (per-id)...", len(cid_list))
    hit = 0
    for i, cid in enumerate(cid_list, 1):
        meta = fetch_market_metadata_one(cid)
        if meta:
            out[cid] = meta
            hit += 1
        if i % 25 == 0:
            log.info("    %d/%d (%d hits)", i, len(cid_list), hit)
        time.sleep(GAMMA_PAUSE_S)
    log.info(
        "  Gamma: %d/%d resolved (%.0f%%)",
        hit, len(cid_list), 100.0 * hit / len(cid_list),
    )
    return out


def _build_lifecycle(
    wallet, alias, cid, oidx, outcome_str, question, end_dt,
    entry_ts, entry_price, entry_shares, entry_cost, entry_tx,
    exit_ts, exit_price, exit_proceeds, exit_tx, exit_type, computed_at,
):
    pnl = (exit_proceeds - entry_cost) if exit_proceeds is not None else None
    pnl_pct = (pnl / entry_cost) if (pnl is not None and entry_cost and entry_cost > 0) else None
    hold_h = ((exit_ts - entry_ts).total_seconds() / 3600.0) if exit_ts else None
    h2r_entry = ((end_dt - entry_ts).total_seconds() / 3600.0) if end_dt else None
    h2r_exit = ((end_dt - exit_ts).total_seconds() / 3600.0) if (end_dt and exit_ts) else None

    id_input = (
        f"{wallet}|{cid}|{oidx}|{entry_tx}|{entry_ts.isoformat()}"
        f"|{round(entry_shares, 4)}"
    )
    row_id = hashlib.sha256(id_input.encode()).hexdigest()[:24]

    return {
        "id": row_id,
        "wallet_address": wallet.lower(),
        "wallet_alias": alias,
        "condition_id": str(cid),
        "market_question": str(question)[:500],
        "market_end_date": end_dt.isoformat() if end_dt else None,
        "outcome": str(outcome_str)[:32],
        "outcome_index": int(oidx),
        "entry_ts": entry_ts.isoformat(),
        "entry_price": round_or_none(entry_price, 6),
        "entry_shares": round_or_none(entry_shares, 4),
        "entry_cost_usd": round_or_none(entry_cost, 4),
        "entry_tx_hash": str(entry_tx) if entry_tx else None,
        "exit_ts": exit_ts.isoformat() if exit_ts else None,
        "exit_price": round_or_none(exit_price, 6),
        "exit_proceeds_usd": round_or_none(exit_proceeds, 4),
        "exit_tx_hash": str(exit_tx) if exit_tx else None,
        "exit_type": exit_type,
        "hold_hours": round_or_none(hold_h, 4),
        "hours_to_resolution_at_entry": round_or_none(h2r_entry, 4),
        "hours_to_resolution_at_exit": round_or_none(h2r_exit, 4),
        "pnl_usd": round_or_none(pnl, 4),
        "pnl_pct": round_or_none(pnl_pct, 6),
        "computed_at": computed_at,
    }


def pair_lifecycles_fifo(wallet, alias, records, market_meta):
    by_market = defaultdict(list)
    for rec in records:
        cid = rec.get("conditionId") or rec.get("condition_id")
        oidx = rec.get("outcomeIndex")
        if cid is None or oidx is None:
            continue
        by_market[(str(cid), int(oidx))].append(rec)

    lifecycles = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for (cid, oidx), trades in by_market.items():
        trades.sort(key=lambda t: t["_parsed_ts"])
        meta = market_meta.get(cid.lower(), {})
        end_dt = parse_ts(meta.get("end_date")) if meta else None
        question = (meta.get("question") if meta else None) or trades[0].get("title") or ""
        outcome_str = trades[0].get("outcome") or ("Yes" if oidx == 0 else "No")

        open_buys = deque()
        for t in trades:
            kind = str(t.get("type") or "").upper()
            side = str(t.get("side") or "").upper()
            ts = t["_parsed_ts"]
            price = float(t.get("price") or 0)
            shares = float(t.get("size") or 0)
            usd = float(t.get("usdcSize") or 0)
            tx = t.get("transactionHash") or ""

            if kind == "TRADE" and side == "BUY":
                if shares > 0 and usd >= MIN_USD_TRADE:
                    open_buys.append({
                        "ts": ts,
                        "price": price,
                        "shares_remaining": shares,
                        "cost_remaining": usd,
                        "tx": tx,
                    })

            elif kind == "TRADE" and side == "SELL":
                shares_to_close = shares
                proceeds_per_share = price
                while shares_to_close > 1e-9 and open_buys:
                    buy = open_buys[0]
                    take = min(buy["shares_remaining"], shares_to_close)
                    if take <= 1e-9:
                        open_buys.popleft()
                        continue
                    cost_taken = buy["cost_remaining"] * (take / buy["shares_remaining"])
                    proceeds = take * proceeds_per_share
                    lifecycles.append(_build_lifecycle(
                        wallet, alias, cid, oidx, outcome_str, question, end_dt,
                        entry_ts=buy["ts"], entry_price=buy["price"],
                        entry_shares=take, entry_cost=cost_taken, entry_tx=buy["tx"],
                        exit_ts=ts, exit_price=proceeds_per_share,
                        exit_proceeds=proceeds, exit_tx=tx, exit_type="sell",
                        computed_at=now_iso,
                    ))
                    buy["shares_remaining"] -= take
                    buy["cost_remaining"] -= cost_taken
                    shares_to_close -= take
                    if buy["shares_remaining"] <= 1e-9:
                        open_buys.popleft()

            elif kind == "REDEEM":
                redeem_price = price
                while open_buys:
                    buy = open_buys.popleft()
                    take = buy["shares_remaining"]
                    if take <= 1e-9:
                        continue
                    proceeds = take * redeem_price
                    lifecycles.append(_build_lifecycle(
                        wallet, alias, cid, oidx, outcome_str, question, end_dt,
                        entry_ts=buy["ts"], entry_price=buy["price"],
                        entry_shares=take, entry_cost=buy["cost_remaining"],
                        entry_tx=buy["tx"], exit_ts=ts, exit_price=redeem_price,
                        exit_proceeds=proceeds, exit_tx=tx,
                        exit_type=("redeem_win" if redeem_price > 0.5 else "redeem_loss"),
                        computed_at=now_iso,
                    ))

        for buy in open_buys:
            if buy["shares_remaining"] <= 1e-9:
                continue
            lifecycles.append(_build_lifecycle(
                wallet, alias, cid, oidx, outcome_str, question, end_dt,
                entry_ts=buy["ts"], entry_price=buy["price"],
                entry_shares=buy["shares_remaining"],
                entry_cost=buy["cost_remaining"], entry_tx=buy["tx"],
                exit_ts=None, exit_price=None, exit_proceeds=None,
                exit_tx=None, exit_type="open", computed_at=now_iso,
            ))

    return lifecycles


def upsert_lifecycle(d1, row):
    cols = list(row.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    update_cols = [c for c in cols if c != "id"]
    set_clause = ", ".join([f"{c}=excluded.{c}" for c in update_cols])
    sql = (
        f"INSERT INTO smart_money_lifecycles ({col_names}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT(id) DO UPDATE SET {set_clause}"
    )
    d1.query(sql, [row[c] for c in cols])


def process_wallet(d1, address, alias):
    log.info("=" * 60)
    log.info("Wallet: %s (%s)", alias, address[:10] + "...")
    log.info("=" * 60)

    log.info("  Fetching /activity (60d, max %d)...", MAX_RECORDS_PER_WALLET)
    records = fetch_wallet_activity(address)
    log.info("  -> %d records", len(records))
    if not records:
        return 0, 0

    cids = {r.get("conditionId") or r.get("condition_id") for r in records}
    cids.discard(None)
    market_meta = fetch_markets_metadata(cids)

    log.info("  Pairing FIFO...")
    lifecycles = pair_lifecycles_fifo(address, alias, records, market_meta)
    closed = sum(1 for lc in lifecycles if lc["exit_ts"] is not None)
    with_meta = sum(1 for lc in lifecycles if lc["market_end_date"] is not None)
    log.info(
        "  -> %d lifecycles (%d closed, %d with end_date metadata)",
        len(lifecycles), closed, with_meta,
    )

    log.info("  Upserting...")
    written = failed = 0
    for i, lc in enumerate(lifecycles, 1):
        try:
            upsert_lifecycle(d1, lc)
            written += 1
            if written % 50 == 0:
                log.info("    %d/%d", written, len(lifecycles))
                time.sleep(0.3)
        except Exception as e:
            failed += 1
            if failed <= 3:
                log.warning("  Upsert error: %s", e)

    log.info("  Wallet done: %d written, %d failed", written, failed)
    return written, failed


def main():
    log.info("=" * 60)
    log.info("JAKUU Smart-Money Lifecycle Dump (S145.2)")
    log.info("=" * 60)

    if not CF_ACCOUNT_ID or not CF_API_TOKEN:
        log.error("Set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN env vars")
        sys.exit(1)

    d1 = D1Client(CF_ACCOUNT_ID, CF_API_TOKEN, D1_DATABASE_ID)

    wallets = fetch_registry_wallets(d1)
    log.info("Registry wallets to process: %d", len(wallets))
    for i, (addr, alias) in enumerate(wallets, 1):
        log.info("  %d. %s (%s)", i, alias, addr[:10] + "...")

    total_written = total_failed = 0
    for address, alias in wallets:
        try:
            w, f = process_wallet(d1, address, alias)
            total_written += w
            total_failed += f
        except Exception as e:
            log.error("Wallet %s failed: %s", alias, e)
            total_failed += 1

    log.info("=" * 60)
    log.info("All wallets done. Total: %d written, %d failed", total_written, total_failed)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
