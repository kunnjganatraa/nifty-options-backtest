#!/usr/bin/env python3
"""
dhan_fetcher.py — Pull REAL expired-options history from Dhan Data API.

Hits POST /v2/charts/rollingoption (rolling ATM-relative expired options).
Returns minute-level OHLC + IV + OI + Volume + Strike + Spot, up to 5 years.

DESIGN PRINCIPLES (the things the old backtest got wrong):
  • FAIL LOUD: missing data is logged to a gaps report, NEVER silently
    replaced with a Black-Scholes guess. A poisoned dataset is worse than none.
  • RESUMABLE: every (symbol, expiry, strike, type, 30-day window) is cached
    to disk as raw JSON. Re-running skips what's already pulled. Safe to Ctrl-C.
  • ONE-TIME PULL: run it once, cache everything, then cancel the Dhan Data API
    sub. The cached parquet is yours forever — backtest offline, free.

USAGE
  export DHAN_CLIENT_ID=xxxxxxxxxx
  export DHAN_ACCESS_TOKEN=eyJ0eX...
  python dhan_fetcher.py --from 2023-01-01 --to 2025-01-01
  python dhan_fetcher.py --from 2023-01-01 --to 2025-01-01 --consolidate-only

  # smaller test first:
  python dhan_fetcher.py --from 2024-12-01 --to 2024-12-15 --symbols NIFTY \
      --strikes 1 --expiries WEEK --dry-run        # shows plan, no calls
"""

import os
import sys
import json
import time
import math
import argparse
import datetime as dt
from pathlib import Path

import requests

# ── CONFIG ──────────────────────────────────────────────────────────────────
BASE        = "https://api.dhan.co/v2"
CLIENT_ID   = os.environ.get("DHAN_CLIENT_ID", "")
ACCESS_TOK  = os.environ.get("DHAN_ACCESS_TOKEN", "")

CACHE_DIR   = Path("dhan_cache")          # raw per-chunk JSON (resumable)
OUT_DIR     = Path("dhan_data")           # consolidated tidy output
GAPS_FILE   = OUT_DIR / "_gaps.jsonl"     # everything that came back empty

# Dhan index security IDs (instrument=OPTIDX, exchangeSegment=NSE_FNO).
# VERIFY against https://dhanhq.co/docs/v2/instruments/ if a symbol returns 0 rows.
UNDERLYINGS = {
    "NIFTY":     {"securityId": 13, "instrument": "OPTIDX", "segment": "NSE_FNO"},
    "BANKNIFTY": {"securityId": 25, "instrument": "OPTIDX", "segment": "NSE_FNO"},
}

# Strikes are ATM-relative. Index near-expiry supports up to ATM±10.
# We build the list as ATM, ATM±1 ... ATM±N from --strikes N.
def strike_labels(n):
    out = ["ATM"]
    for i in range(1, n + 1):
        out += [f"ATM+{i}", f"ATM-{i}"]
    return out

REQUIRED = ["open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot"]
MAX_WINDOW_DAYS = 30          # API hard limit per call
SLEEP_BETWEEN   = 0.35        # polite throttle (sec); minute data has no hard cap
MAX_RETRIES     = 4
NET_FAIL        = object()    # sentinel: couldn't reach server (not a data gap)


# ── HTTP ──────────────────────────────────────────────────────────────────--
def _headers():
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "access-token": ACCESS_TOK,
        "client-id": CLIENT_ID,
    }


def check_profile():
    """Verify token + Data API subscription before burning time. Non-fatal."""
    try:
        r = requests.get(f"{BASE}/profile", headers=_headers(), timeout=15)
        if r.status_code == 200:
            p = r.json()
            print(f"  [PROFILE] token OK | dataApi={p.get('dataValidity', p.get('ddpi','?'))} "
                  f"segments={p.get('activeSegment', p.get('tradingApi','?'))}")
            return True
        print(f"  [PROFILE] {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"  [PROFILE] check skipped: {e}")
    return False


def fetch_rolling(symbol, meta, expiry_flag, expiry_code, strike, opt_type,
                  interval, from_d, to_d):
    """One rollingoption call. Returns parsed dict or {} on empty/error."""
    body = {
        "exchangeSegment": meta["segment"],
        "interval":        str(interval),
        "securityId":      meta["securityId"],
        "instrument":      meta["instrument"],
        "expiryFlag":      expiry_flag,        # WEEK | MONTH
        "expiryCode":      expiry_code,        # 0 = nearest, 1 = next, ...
        "strike":          strike,             # ATM | ATM+n | ATM-n
        "drvOptionType":   "CALL" if opt_type == "CE" else "PUT",
        "requiredData":    REQUIRED,
        "fromDate":        str(from_d),
        "toDate":          str(to_d),          # non-inclusive
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.post(f"{BASE}/charts/rollingoption",
                              headers=_headers(), json=body, timeout=30)
            if r.status_code == 200:
                return r.json().get("data", {}) or {}
            if r.status_code in (429, 805):           # rate limited
                wait = 2 ** attempt
                print(f"    [429] backing off {wait}s")
                time.sleep(wait); continue
            if r.status_code == 401:
                sys.exit("  [FATAL] 401 — token expired/invalid. Regenerate and re-run.")
            print(f"    [API {r.status_code}] {r.text[:160]}")
            return {}                       # genuine bad-param / no data → real gap
        except requests.RequestException as e:
            wait = 2 ** attempt
            print(f"    [NET] {str(e)[:90]} — retry in {wait}s")
            time.sleep(wait)
    return NET_FAIL                          # couldn't reach server → NOT a data gap


# ── PARSE (columnar → rows) ───────────────────────────────────────────────--
def columnar_to_rows(leg):
    """Dhan returns parallel arrays per field. Zip them into row dicts."""
    if not leg:
        return []
    ts = leg.get("timestamp") or []
    n  = len(ts)
    if n == 0:
        return []
    def col(name):
        v = leg.get(name) or []
        return v + [None] * (n - len(v))      # pad short arrays defensively
    cols = {f: col(f) for f in ["open", "high", "low", "close",
                                 "iv", "oi", "volume", "strike", "spot"]}
    rows = []
    for i in range(n):
        rows.append({
            "ts":     ts[i],
            "open":   cols["open"][i],   "high":   cols["high"][i],
            "low":    cols["low"][i],    "close":  cols["close"][i],
            "iv":     cols["iv"][i],     "oi":     cols["oi"][i],
            "volume": cols["volume"][i], "strike": cols["strike"][i],
            "spot":   cols["spot"][i],
        })
    return rows


# ── WINDOWS ───────────────────────────────────────────────────────────────--
def windows(from_d, to_d, days=MAX_WINDOW_DAYS):
    cur = from_d
    while cur < to_d:
        end = min(cur + dt.timedelta(days=days), to_d)
        yield cur, end
        cur = end


def chunk_key(symbol, ef, ec, strike, opt, interval, a, b):
    s = strike.replace("+", "p").replace("-", "m")
    return f"{symbol}_{ef}_ec{ec}_{s}_{opt}_{interval}m_{a}_{b}.json"


# ── MAIN PULL ─────────────────────────────────────────────────────────────--
def run(args):
    CACHE_DIR.mkdir(exist_ok=True)
    OUT_DIR.mkdir(exist_ok=True)

    from_d = dt.date.fromisoformat(args.from_date)
    to_d   = dt.date.fromisoformat(args.to_date)
    syms   = args.symbols or list(UNDERLYINGS)
    strikes = strike_labels(args.strikes)
    expiries = args.expiries or ["WEEK", "MONTH"]
    opts   = ["CE", "PE"]

    plan = []
    for sym in syms:
        for ef in expiries:
            for strike in strikes:
                for opt in opts:
                    for a, b in windows(from_d, to_d):
                        plan.append((sym, ef, strike, opt, a, b))

    print(f"\n[PLAN] {len(plan)} chunks | symbols={syms} expiries={expiries} "
          f"strikes={strikes} interval={args.interval}m range={from_d}..{to_d}")
    if args.dry_run:
        print("[DRY-RUN] no calls made."); return

    if not (CLIENT_ID and ACCESS_TOK):
        sys.exit("[FATAL] set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN env vars.")
    check_profile()

    done = empty = pulled = 0
    for sym, ef, strike, opt, a, b in plan:
        meta = UNDERLYINGS[sym]
        ck   = CACHE_DIR / chunk_key(sym, ef, args.expiry_code, strike, opt, args.interval, a, b)
        if ck.exists():                       # resume
            done += 1; continue

        # Fetch; if the network is down, WAIT and retry the same chunk rather
        # than recording a false gap. Self-heals when wifi comes back.
        data = fetch_rolling(sym, meta, ef, args.expiry_code, strike, opt,
                             args.interval, a, b)
        net_waits = 0
        while data is NET_FAIL:
            net_waits += 1
            print(f"  [NET DOWN] no connection — waiting 30s (attempt {net_waits}). "
                  f"Reconnect wifi; run will resume automatically.")
            time.sleep(30)
            data = fetch_rolling(sym, meta, ef, args.expiry_code, strike, opt,
                                 args.interval, a, b)

        leg  = data.get("ce" if opt == "CE" else "pe")
        rows = columnar_to_rows(leg)

        if not rows:                          # genuine empty (real data gap)
            empty += 1
            with open(GAPS_FILE, "a") as g:
                g.write(json.dumps({"symbol": sym, "expiryFlag": ef, "strike": strike,
                                    "opt": opt, "from": str(a), "to": str(b)}) + "\n")
        else:
            pulled += 1
            ck.write_text(json.dumps({"meta": {"symbol": sym, "expiryFlag": ef,
                                               "strike": strike, "opt": opt,
                                               "interval": args.interval},
                                      "rows": rows}))
        if (done + pulled + empty) % 25 == 0:
            print(f"  ...{done} cached, {pulled} pulled, {empty} empty")
        time.sleep(SLEEP_BETWEEN)

    print(f"\n[PULL DONE] cached={done} pulled={pulled} empty/gaps={empty}")
    print(f"  gaps logged → {GAPS_FILE}")
    consolidate()


# ── CONSOLIDATE → tidy table ──────────────────────────────────────────────--
def consolidate():
    """Merge all cached chunks into one tidy file (parquet if pyarrow, else csv)."""
    files = sorted(CACHE_DIR.glob("*.json"))
    if not files:
        print("[CONSOLIDATE] no cached chunks yet."); return
    print(f"[CONSOLIDATE] merging {len(files)} chunks...")
    try:
        import pandas as pd
    except ImportError:
        sys.exit("[FATAL] pip install pandas (and pyarrow for parquet).")

    frames = []
    for f in files:
        d = json.loads(f.read_text())
        m = d["meta"]; df = pd.DataFrame(d["rows"])
        if df.empty:
            continue
        df["symbol"]      = m["symbol"]
        df["expiry_flag"] = m["expiryFlag"]
        df["strike_label"] = m["strike"]
        df["opt"]         = m["opt"]
        df["datetime"]    = pd.to_datetime(df["ts"], unit="s", utc=True)\
                              .dt.tz_convert("Asia/Kolkata")
        frames.append(df)
    if not frames:
        print("[CONSOLIDATE] all chunks empty."); return
    out = pd.concat(frames, ignore_index=True)
    # Drop duplicate bars from overlapping windows (e.g. test-run chunks).
    key = ["symbol", "expiry_flag", "opt", "strike_label", "strike", "datetime"]
    before = len(out)
    out = out.drop_duplicates(subset=[c for c in key if c in out.columns], keep="last")
    if before != len(out):
        print(f"[CONSOLIDATE] dropped {before - len(out):,} duplicate rows")
    out = out.sort_values(["symbol", "expiry_flag", "opt", "strike_label", "datetime"])
    cols = ["datetime", "symbol", "expiry_flag", "opt", "strike_label",
            "strike", "spot", "open", "high", "low", "close", "iv", "oi", "volume"]
    out = out[[c for c in cols if c in out.columns]]

    try:
        path = OUT_DIR / "options_history.parquet"
        out.to_parquet(path, index=False)
    except Exception:
        path = OUT_DIR / "options_history.csv"
        out.to_csv(path, index=False)
    print(f"[CONSOLIDATE] {len(out):,} rows → {path}")
    print(out.groupby(["symbol", "expiry_flag", "opt"]).size().to_string())


# ── CLI ─────────────────────────────────────────────────────────────────--
def inspect():
    """Print schema, range, and a sample day so the engine is built on reality."""
    try:
        import pandas as pd
    except ImportError:
        sys.exit("[FATAL] pip install pandas pyarrow")
    p = OUT_DIR / "options_history.parquet"
    if not p.exists():
        p = OUT_DIR / "options_history.csv"
    if not p.exists():
        sys.exit("[INSPECT] no consolidated file yet — run --consolidate-only first.")
    print(f"[INSPECT] reading {p.name} ...")
    df = pd.read_parquet(p) if p.suffix == ".parquet" else pd.read_csv(p, parse_dates=["datetime"])
    print(f"\nrows: {len(df):,}")
    print(f"date range: {df['datetime'].min()}  →  {df['datetime'].max()}")
    print(f"\ncolumns / dtypes:\n{df.dtypes.to_string()}")
    print(f"\nstrike_labels present: {sorted(df['strike_label'].unique())}")
    # one sample day: NIFTY weekly ATM CE+PE, first ~6 bars
    s = df[(df.symbol == "NIFTY") & (df.expiry_flag == "WEEK") &
           (df.strike_label == "ATM")].sort_values("datetime").head(6)
    print(f"\nsample — NIFTY WEEK ATM (first 6 bars):\n{s.to_string(index=False)}")
    # how spot/strike move within one day for ATM (rolling check)
    day = df[(df.symbol == "NIFTY") & (df.expiry_flag == "WEEK") &
             (df.strike_label == "ATM") & (df.opt == "CE")].copy()
    if len(day):
        day["d"] = day["datetime"].dt.date
        g = day.groupby("d")["strike"].nunique()
        print(f"\nATM strike changes/day (NIFTY WEEK CE) — sample:\n{g.head(8).to_string()}")


def main():
    global CLIENT_ID, ACCESS_TOK
    # Load a .env file sitting next to this script (or in the current folder).
    # Keeps secrets out of shell history. Falls back to plain env vars.
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).with_name(".env"))
        load_dotenv()
    except ImportError:
        pass
    CLIENT_ID  = os.environ.get("DHAN_CLIENT_ID",  CLIENT_ID)
    ACCESS_TOK = os.environ.get("DHAN_ACCESS_TOKEN", ACCESS_TOK)

    ap = argparse.ArgumentParser(description="Dhan expired-options fetcher")
    ap.add_argument("--from", dest="from_date", required=False, default="2023-01-01")
    ap.add_argument("--to",   dest="to_date",   required=False,
                    default=str(dt.date.today()))
    ap.add_argument("--symbols", nargs="*", choices=list(UNDERLYINGS),
                    help="default: NIFTY BANKNIFTY")
    ap.add_argument("--expiries", nargs="*", choices=["WEEK", "MONTH"],
                    help="default: WEEK MONTH")
    ap.add_argument("--strikes", type=int, default=5,
                    help="N → ATM, ATM±1..ATM±N (index supports up to 10)")
    ap.add_argument("--expiry-code", type=int, default=1,
                    help="1 = nearest expiry (rolling front), 2 = next, ... (NOT 0-indexed)")
    ap.add_argument("--interval", type=int, default=1, choices=[1, 5, 15, 25, 60])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--consolidate-only", action="store_true")
    ap.add_argument("--inspect", action="store_true",
                    help="print schema + sample of the consolidated file")
    args = ap.parse_args()

    if args.inspect:
        inspect()
    elif args.consolidate_only:
        consolidate()
    else:
        run(args)


if __name__ == "__main__":
    main()
