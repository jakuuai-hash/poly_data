#!/usr/bin/env python3
"""
JAKUU Smart-Money Lifecycle Dump — S147 (corrigendum patch)

Pulls /activity history for every wallet in smart_money_wallets,
pairs entries to exits via FIFO, joins with Gamma market metadata
(endDate, event_id, resolution), and upserts complete lifecycles
into smart_money_lifecycles.

S147 changes vs S145.2 (per JAKUU_MIRROR_FRAMEWORK.md v1.2):
  (A) Gamma metadata now ALSO returns event_id (markets[0].events[0].id),
      `closed` flag, and parsed outcomePrices for resolution detection.
      No second API call required — events nest under /markets response.
      Spec said "fetch event_id from /events/by-condition"; the live
      Gamma /markets?conditionIds=X already returns events nested. Saves
      one round-trip per condition_id vs the original spec.
  (B) Synthetic Gamma-derived REDEEM lifecycles for unpaired open buys
      where the underlying market has resolved. Fixes the survivorship
      bias documented in S146: PM /activity exposes only TRADE events,
      never REDEEM, so hold-to-redeem losers had ZERO rows in the
      lifecycle table. Now closed at exit_price ∈ {0.0, 1.0} based on
      Gamma `closed=true` + `outcomePrices`. exit_type:
        "redeem_synth_win"  — wallet's outcome won  (price=1.0)
        "redeem_synth_loss" — wallet's outcome lost (price=0.0)
      These are SYNTHETIC; exit_tx_hash=NULL. Distinguishable from
      real on-chain REDEEMs (kept as exit_type="redeem_win"/"redeem_loss")
      so analyses can opt to include or exclude.
  (C) Lifecycle row schema: `event_id TEXT` field added. Requires
      D1 migration M1.6.A run BEFORE this script (see migration sql
      header in repo). Existing rows get NULL until re-dumped.
  (D) Resolution detection threshold: matches the framework spec —
      `closed == True` AND outcomePrices[winner] >= 0.95 (or <= 0.05
      on the loser side). Prevents counting markets that "closed" for
      trading but are awaiting UMA finalization.

S145.2 changes preserved:
  (1) Per-conditionId Gamma loop (batch query is broken).
  (2) Wallet priority ordering.
  (3) MAX_RECORDS_PER_WALLET cap.
  (4) Streaming upsert.

Compatibility: this script REQUIRES the M1.6.A migration. Run it first:
  ALTER TABLE smart_money_lifecycles ADD COLUMN event_id TEXT;
  CREATE INDEX IF NOT EXISTS idx_sml_wallet_event
    ON smart_money_lifecycles(wallet_address, event_id);
"""

import os, sys, time, json, hashlib, logging
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
    Per-id Gamma lookup. Returns endDate, question, event_id, and
    resolution state for synthetic-REDEEM synthesis.

    Per-id is slower but correct: the batch path
    (?conditionIds=A&conditionIds=B) returns ≤1 result when conditionIds
    is repeated — verified broken in S145.1.

    S147 additions (corrigendum patch):
      - event_id from m.events[0].id (events array nests under markets;
        no second /events call required).
      - closed: bool — Gamma `closed` flag (orderbook closed for trades).
      - resolved_yes: True/False/None — derived from `outcomePrices`
        with >=0.95 / <=0.05 threshold. None when market is not cleanly
        resolved (still open, awaiting UMA, or borderline).
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

        # event_id: markets nest events under .events[]; canonical
        # path is events[0].id. Defensive guard for shape variance.
        event_id = None
        evs = m.get("events")
        if isinstance(evs, list) and evs:
            ev0 = evs[0]
            if isinstance(ev0, dict):
                eid = ev0.get("id")
                if eid is not None:
                    event_id = str(eid)

        # Resolution state. Only mark resolved_yes for clean resolutions
        # (outcome price >= 0.95 / <= 0.05). Markets that "closed" but
        # await UMA finalization stay resolved_yes=None.
        closed_flag = bool(m.get("closed"))
        resolved_yes = None
        if closed_flag:
            try:
                op_raw = m.get("outcomePrices")
                op = json.loads(op_raw) if isinstance(op_raw, str) else op_raw
                if isinstance(op, list) and len(op) >= 2:
                    yes_p = float(op[0])
                    if yes_p >= 0.95:
                        resolved_yes = True
                    elif yes_p <= 0.05:
                        resolved_yes = False
                    # else: borderline, leave as None
            except (ValueError, TypeError, json.JSONDecodeError):
                pass

        return {
            "end_date": m.get("endDate") or m.get("end_date_iso"),
            "question": m.get("question"),
            "event_id": event_id,
            "closed": closed_flag,
            "resolved_yes": resolved_yes,
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
    n = len(cid_list)
    with_event = sum(1 for v in out.values() if v.get("event_id"))
    resolved = sum(1 for v in out.values() if v.get("resolved_yes") is not None)
    log.info(
        "  Gamma: %d/%d hit (%.0f%%), %d with event_id (%.0f%%), "
        "%d cleanly resolved (%.0f%%)",
        hit, n, 100.0 * hit / n if n else 0,
        with_event, 100.0 * with_event / n if n else 0,
        resolved, 100.0 * resolved / n if n else 0,
    )
    return out


def _build_lifecycle(
    wallet, alias, cid, oidx, outcome_str, question, end_dt, event_id,
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
        "event_id": str(event_id) if event_id else None,
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
    synth_redeem_count = 0  # track synthetic-REDEEM emission for logging

    for (cid, oidx), trades in by_market.items():
        trades.sort(key=lambda t: t["_parsed_ts"])
        meta = market_meta.get(cid.lower(), {})
        end_dt = parse_ts(meta.get("end_date")) if meta else None
        question = (meta.get("question") if meta else None) or trades[0].get("title") or ""
        outcome_str = trades[0].get("outcome") or ("Yes" if oidx == 0 else "No")
        event_id = (meta.get("event_id") if meta else None)
        market_closed = bool(meta.get("closed")) if meta else False
        resolved_yes = meta.get("resolved_yes") if meta else None

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
                        wallet, alias, cid, oidx, outcome_str, question, end_dt, event_id,
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
                # On-chain REDEEM event surfaced in /activity. Rare —
                # /activity historically returns only TRADE events. If it
                # does appear, prefer it over our synthetic version.
                redeem_price = price
                while open_buys:
                    buy = open_buys.popleft()
                    take = buy["shares_remaining"]
                    if take <= 1e-9:
                        continue
                    proceeds = take * redeem_price
                    lifecycles.append(_build_lifecycle(
                        wallet, alias, cid, oidx, outcome_str, question, end_dt, event_id,
                        entry_ts=buy["ts"], entry_price=buy["price"],
                        entry_shares=take, entry_cost=buy["cost_remaining"],
                        entry_tx=buy["tx"], exit_ts=ts, exit_price=redeem_price,
                        exit_proceeds=proceeds, exit_tx=tx,
                        exit_type=("redeem_win" if redeem_price > 0.5 else "redeem_loss"),
                        computed_at=now_iso,
                    ))

        # ─── Unpaired-buy resolution (S147 corrigendum patch) ─────────
        # Any buy still open in the deque at this point was not paired
        # to a SELL or on-chain REDEEM. Three cases:
        #   (a) Gamma says the market resolved cleanly → synthesize
        #       a redeem_synth_{win,loss} lifecycle at exit_price ∈ {0,1}
        #       per the wallet's outcome side. exit_tx_hash=NULL marks
        #       it as synthesized.
        #   (b) Gamma says the market is closed but resolution is
        #       borderline / awaiting UMA → leave as exit_type="open".
        #       Better to under-count than to mislabel.
        #   (c) Gamma says the market is still open → leave as "open".
        for buy in open_buys:
            if buy["shares_remaining"] <= 1e-9:
                continue

            # Default = open. Only override if market is cleanly resolved.
            exit_type_synth = "open"
            exit_price_synth = None
            exit_proceeds_synth = None
            exit_ts_synth = None

            if market_closed and resolved_yes is not None:
                # Wallet's outcome side: oidx=0 == YES, oidx=1 == NO.
                wallet_won = (resolved_yes is True and oidx == 0) or \
                             (resolved_yes is False and oidx == 1)
                exit_price_synth = 1.0 if wallet_won else 0.0
                exit_proceeds_synth = buy["shares_remaining"] * exit_price_synth
                # Use market endDate as the exit timestamp when known;
                # fall back to computed_at. This keeps hold_hours sensible.
                exit_ts_synth = end_dt if end_dt else datetime.now(timezone.utc)
                exit_type_synth = "redeem_synth_win" if wallet_won else "redeem_synth_loss"
                synth_redeem_count += 1

            lifecycles.append(_build_lifecycle(
                wallet, alias, cid, oidx, outcome_str, question, end_dt, event_id,
                entry_ts=buy["ts"], entry_price=buy["price"],
                entry_shares=buy["shares_remaining"],
                entry_cost=buy["cost_remaining"], entry_tx=buy["tx"],
                exit_ts=exit_ts_synth, exit_price=exit_price_synth,
                exit_proceeds=exit_proceeds_synth,
                exit_tx=None,  # synthetic — no on-chain tx
                exit_type=exit_type_synth, computed_at=now_iso,
            ))

    if synth_redeem_count:
        log.info("    Synthetic REDEEMs emitted: %d", synth_redeem_count)
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
    with_event = sum(1 for lc in lifecycles if lc.get("event_id"))
    synth_redeems = sum(
        1 for lc in lifecycles
        if lc.get("exit_type") in ("redeem_synth_win", "redeem_synth_loss")
    )
    distinct_events = len({lc["event_id"] for lc in lifecycles if lc.get("event_id")})
    distinct_cids = len({lc["condition_id"] for lc in lifecycles})
    log.info(
        "  -> %d lifecycles (%d closed, %d with end_date, %d with event_id)",
        len(lifecycles), closed, with_meta, with_event,
    )
    log.info(
        "     %d synthetic REDEEMs | %d distinct events | %d distinct condition_ids "
        "(collapse ratio %.2fx)",
        synth_redeems, distinct_events, distinct_cids,
        (distinct_cids / distinct_events) if distinct_events else 0.0,
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
