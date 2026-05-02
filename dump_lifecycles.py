#!/usr/bin/env python3
"""
JAKUU Lifecycle Dump (S145.1) — companion to smart_money_api.py

S145.1 changes vs S145:
  * MAX_RECORDS_PER_WALLET cap (default 15000) — skips MM wallets that
    would eat the GitHub Actions timeout budget.
  * Per-wallet streaming upsert — pair and write per wallet, not all at
    end. If timeout hits mid-run, partial data is already persisted.
"""

import os, sys, time, hashlib, logging, argparse
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque

import requests

D1_DATABASE_ID = os.getenv("D1_DATABASE_ID", "85b521ad-69d2-4713-be3f-ad2e3ffcca6b")
CF_ACCOUNT_ID = os.getenv("CLOUDFLARE_ACCOUNT_ID", "")
CF_API_TOKEN = os.getenv("CLOUDFLARE_API_TOKEN", "")

DATA_API_BASE = "https://data-api.polymarket.com"
GAMMA_API_BASE = "https://gamma-api.polymarket.com"

DEFAULT_LOOKBACK_DAYS = 60
PAGE_SIZE = 500
MAX_RECORDS_PER_WALLET = 15000
MIN_USD_TRADE = 1.0
GAMMA_BATCH_SIZE = 50
REQUEST_PAUSE_S = 0.3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("dump_lifecycles")

SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS smart_money_lifecycles (
        id TEXT PRIMARY KEY,
        wallet_address TEXT NOT NULL,
        wallet_alias TEXT,
        condition_id TEXT NOT NULL,
        market_question TEXT,
        market_end_date TEXT,
        outcome TEXT,
        outcome_index INTEGER,
        entry_ts TEXT NOT NULL,
        entry_price REAL,
        entry_shares REAL,
        entry_cost_usd REAL,
        entry_tx_hash TEXT,
        exit_ts TEXT,
        exit_price REAL,
        exit_proceeds_usd REAL,
        exit_tx_hash TEXT,
        exit_type TEXT,
        hold_hours REAL,
        hours_to_resolution_at_entry REAL,
        hours_to_resolution_at_exit REAL,
        pnl_usd REAL,
        pnl_pct REAL,
        computed_at TEXT NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_sml_wallet ON smart_money_lifecycles(wallet_address)",
    "CREATE INDEX IF NOT EXISTS idx_sml_market ON smart_money_lifecycles(condition_id)",
    "CREATE INDEX IF NOT EXISTS idx_sml_entry_ts ON smart_money_lifecycles(entry_ts DESC)",
    "CREATE INDEX IF NOT EXISTS idx_sml_h2r_entry ON smart_money_lifecycles(hours_to_resolution_at_entry)",
]


class D1Client:
    def __init__(self, account_id, api_token, database_id):
        self.url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
        self.headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}

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


def fetch_wallet_activity(address, lookback_days, max_records):
    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    cutoff_epoch = int(cutoff.timestamp())
    all_records = []
    seen_tx = set()
    end_param = None
    page = 0
    truncated = False

    while True:
        page += 1
        params = {
            "user": address, "type": "TRADE,REDEEM",
            "limit": PAGE_SIZE, "sortBy": "TIMESTAMP", "sortDirection": "DESC",
        }
        if end_param is not None:
            params["end"] = end_param
        try:
            r = requests.get(f"{DATA_API_BASE}/activity", params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning("    /activity error for %s p%d: %s", address[:10], page, e)
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
                return all_records, truncated
            seen_tx.add(tx)
            item["_parsed_ts"] = ts
            all_records.append(item)
            new_count += 1
            if earliest_ts is None or ts < earliest_ts:
                earliest_ts = ts
            if len(all_records) >= max_records:
                log.warning("    HIT MAX_RECORDS cap (%d) — likely MM wallet, stopping", max_records)
                truncated = True
                return all_records, truncated

        if len(items) < PAGE_SIZE or new_count == 0 or earliest_ts is None:
            break
        next_end = int(earliest_ts.timestamp()) - 1
        if next_end < cutoff_epoch:
            break
        end_param = next_end
        time.sleep(REQUEST_PAUSE_S)

    return all_records, truncated


def fetch_markets_metadata(condition_ids):
    out = {}
    cid_list = sorted({c.lower() for c in condition_ids if c})
    for i in range(0, len(cid_list), GAMMA_BATCH_SIZE):
        batch = cid_list[i:i + GAMMA_BATCH_SIZE]
        params = [("conditionIds", c) for c in batch]
        params.append(("limit", str(GAMMA_BATCH_SIZE)))
        try:
            r = requests.get(f"{GAMMA_API_BASE}/markets", params=params, timeout=30)
            r.raise_for_status()
            for m in (r.json() or []):
                cid = m.get("conditionId")
                if not cid:
                    continue
                out[str(cid).lower()] = {
                    "end_date": m.get("endDate") or m.get("end_date_iso"),
                    "question": m.get("question"),
                }
        except Exception as e:
            log.warning("    Gamma fetch failed for batch %d: %s", i // GAMMA_BATCH_SIZE, e)
        time.sleep(REQUEST_PAUSE_S)
    return out


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
                    open_buys.append({"ts": ts, "price": price,
                                      "shares_remaining": shares,
                                      "cost_remaining": usd, "tx": tx})
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
                        entry_ts=buy["ts"], entry_price=buy["price"], entry_shares=take,
                        entry_cost=cost_taken, entry_tx=buy["tx"],
                        exit_ts=ts, exit_price=proceeds_per_share,
                        exit_proceeds=proceeds, exit_tx=tx, exit_type="sell",
                        computed_at=now_iso))
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
                        entry_ts=buy["ts"], entry_price=buy["price"], entry_shares=take,
                        entry_cost=buy["cost_remaining"], entry_tx=buy["tx"],
                        exit_ts=ts, exit_price=redeem_price, exit_proceeds=proceeds,
                        exit_tx=tx, exit_type=("redeem_win" if redeem_price > 0.5 else "redeem_loss"),
                        computed_at=now_iso))

        for buy in open_buys:
            if buy["shares_remaining"] <= 1e-9:
                continue
            lifecycles.append(_build_lifecycle(
                wallet, alias, cid, oidx, outcome_str, question, end_dt,
                entry_ts=buy["ts"], entry_price=buy["price"],
                entry_shares=buy["shares_remaining"], entry_cost=buy["cost_remaining"],
                entry_tx=buy["tx"], exit_ts=None, exit_price=None, exit_proceeds=None,
                exit_tx=None, exit_type="open", computed_at=now_iso))

    return lifecycles


def _build_lifecycle(wallet, alias, cid, oidx, outcome_str, question, end_dt,
                     entry_ts, entry_price, entry_shares, entry_cost, entry_tx,
                     exit_ts, exit_price, exit_proceeds, exit_tx, exit_type, computed_at):
    pnl = (exit_proceeds - entry_cost) if exit_proceeds is not None else None
    pnl_pct = (pnl / entry_cost) if (pnl is not None and entry_cost and entry_cost > 0) else None
    hold_h = ((exit_ts - entry_ts).total_seconds() / 3600.0) if exit_ts else None
    h2r_entry = ((end_dt - entry_ts).total_seconds() / 3600.0) if end_dt else None
    h2r_exit = ((end_dt - exit_ts).total_seconds() / 3600.0) if (end_dt and exit_ts) else None

    id_input = f"{wallet}|{cid}|{oidx}|{entry_tx}|{entry_ts.isoformat()}|{round(entry_shares, 4)}"
    row_id = hashlib.sha256(id_input.encode()).hexdigest()[:24]

    return {
        "id": row_id, "wallet_address": wallet.lower(), "wallet_alias": alias,
        "condition_id": str(cid), "market_question": str(question)[:500],
        "market_end_date": end_dt.isoformat() if end_dt else None,
        "outcome": str(outcome_str)[:32], "outcome_index": int(oidx),
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


def fetch_registry_wallets(d1):
    res = d1.query("SELECT address, alias FROM smart_money_wallets")
    return [(row["address"].lower(), row.get("alias") or row["address"][:10])
            for row in (res.get("results") or [])]


def upsert_lifecycle(d1, row):
    cols = list(row.keys())
    placeholders = ", ".join(["?"] * len(cols))
    col_names = ", ".join(cols)
    update_cols = [c for c in cols if c != "id"]
    set_clause = ", ".join([f"{c}=excluded.{c}" for c in update_cols])
    sql = (f"INSERT INTO smart_money_lifecycles ({col_names}) VALUES ({placeholders}) "
           f"ON CONFLICT(id) DO UPDATE SET {set_clause}")
    d1.query(sql, [row[c] for c in cols])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    p.add_argument("--max-records", type=int, default=MAX_RECORDS_PER_WALLET)
    p.add_argument("--commit", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--wallet", help="Limit to single wallet address (hex)")
    args = p.parse_args()
    dry_run = args.dry_run or not args.commit

    log.info("=" * 60)
    log.info("JAKUU Lifecycle Dump (S145.1)")
    log.info("=" * 60)
    log.info("Lookback: %d days | Max records/wallet: %d | Mode: %s",
             args.lookback_days, args.max_records,
             "DRY-RUN" if dry_run else "COMMIT")

    if not dry_run and (not CF_ACCOUNT_ID or not CF_API_TOKEN):
        log.error("Set CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN env vars")
        sys.exit(1)

    d1 = D1Client(CF_ACCOUNT_ID, CF_API_TOKEN, D1_DATABASE_ID) if not dry_run else None

    if not dry_run:
        log.info("Ensuring smart_money_lifecycles table + indexes exist...")
        for stmt in SCHEMA_STATEMENTS:
            d1.query(stmt)

    if args.wallet:
        wallets = [(args.wallet.lower(), "ad-hoc")]
    elif dry_run:
        if CF_ACCOUNT_ID and CF_API_TOKEN:
            wallets = fetch_registry_wallets(D1Client(CF_ACCOUNT_ID, CF_API_TOKEN, D1_DATABASE_ID))
        else:
            wallets = [("0xc6587b11a2209e46dfe3928b31c5514a8e33b784", "Erasmus.")]
    else:
        wallets = fetch_registry_wallets(d1)
    log.info("Wallets to process: %d", len(wallets))

    market_meta = {}
    cids_resolved = set()
    grand = {"lc": 0, "ok": 0, "skip": 0, "ups": 0, "fail": 0}

    for idx, (addr, alias) in enumerate(wallets, 1):
        log.info("")
        log.info("[%d/%d] %s (%s)...", idx, len(wallets), alias, addr[:10])
        try:
            records, truncated = fetch_wallet_activity(addr, args.lookback_days, args.max_records)
        except Exception as e:
            log.warning("  Fetch failed: %s", e)
            grand["skip"] += 1
            continue

        log.info("  -> %d records%s", len(records), " (TRUNCATED)" if truncated else "")
        if truncated:
            log.info("  Skipping MM wallet (oracle_signal-only)")
            grand["skip"] += 1
            continue
        if not records:
            grand["ok"] += 1
            continue

        wallet_cids = {r.get("conditionId") or r.get("condition_id") for r in records}
        wallet_cids.discard(None)
        new_cids = {str(c) for c in wallet_cids if str(c).lower() not in cids_resolved}
        if new_cids:
            log.info("  Fetching Gamma metadata for %d new markets...", len(new_cids))
            market_meta.update(fetch_markets_metadata(new_cids))
            cids_resolved.update(c.lower() for c in new_cids)

        lifecycles = pair_lifecycles_fifo(addr, alias, records, market_meta)
        log.info("  -> %d lifecycles paired", len(lifecycles))
        grand["lc"] += len(lifecycles)

        if dry_run or not lifecycles:
            grand["ok"] += 1
            continue

        log.info("  Upserting...")
        written = 0; failed = 0
        for lc in lifecycles:
            try:
                upsert_lifecycle(d1, lc)
                written += 1
                if written % 50 == 0:
                    time.sleep(0.3)
            except Exception as e:
                failed += 1
                if failed <= 3:
                    log.warning("    Upsert error: %s", e)
        log.info("  -> %d upserted, %d failed", written, failed)
        grand["ups"] += written; grand["fail"] += failed; grand["ok"] += 1
        time.sleep(0.3)

    log.info("")
    log.info("=" * 60)
    log.info("Done. Processed: %d | Skipped MM: %d | Lifecycles: %d | Upserts: %d | Failed: %d",
             grand["ok"], grand["skip"], grand["lc"], grand["ups"], grand["fail"])


if __name__ == "__main__":
    main()
