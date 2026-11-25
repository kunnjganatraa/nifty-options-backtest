#!/usr/bin/env python3
"""
strangle_backtest.py — Weekly short-strangle backtest on EXACT Upstox 5-min prices.

Rule (per your spec):
  - Expiry: weekly with >=7 DTE (already encoded in the fetched legs).
  - PUT:  strike = round(spot - 750).  Entry premium = P.
  - CALL: >=600 OTM, within 150 pts of the put's distance (=> 600-900 OTM),
          pick the strike whose premium is CLOSEST to P. Flag if >10% off P.
  - Entry 15:15 (sell both), exit 09:20 next day (buy back). Overnight hold.

Reads upstox_data/strangle_legs.parquet  (date, expiry, dte, opt, strike, spot,
px_entry_1515, px_exit_0920_next).

USAGE
  python strangle_backtest.py
  python strangle_backtest.py --lot 75 --cost-points 2.0

OUTPUT
  backtest_out/strangle_trades.csv   (per-trade log)
  printed summary metrics
"""
import argparse, math
from pathlib import Path
import pandas as pd

LEGS = Path("upstox_data/strangle_legs.parquet")
OUT  = Path("backtest_out"); OUT.mkdir(exist_ok=True)
PUT_OFFSET = 750
CALL_MIN_OTM = 600
MAX_DIST_DIFF = 150        # |put_dist - call_dist| <= 150
PREM_TOL = 0.10            # flag call if >10% off put premium


def build_trades(df):
    trades = []
    for d, g in df.groupby("date"):
        spot = float(g["spot"].iloc[0])
        puts = g[(g.opt == "PE") & g.px_entry_1515.notna()]
        calls = g[(g.opt == "CE") & g.px_entry_1515.notna()]
        if puts.empty or calls.empty:
            continue
        # PUT: strike closest to spot-750
        target = spot - PUT_OFFSET
        prow = puts.iloc[(puts.strike - target).abs().argmin()]
        P = float(prow.px_entry_1515)
        put_dist = spot - prow.strike

        # CALL candidates: >=600 OTM and within 150 of put distance
        cc = calls[(calls.strike - spot >= CALL_MIN_OTM) &
                   ((calls.strike - spot - put_dist).abs() <= MAX_DIST_DIFF)]
        if cc.empty:
            cc = calls[calls.strike - spot >= CALL_MIN_OTM]      # relax to >=600
        if cc.empty:
            continue
        crow = cc.iloc[(cc.px_entry_1515 - P).abs().argmin()]    # closest premium
        C = float(crow.px_entry_1515)
        flag = abs(C - P) / P > PREM_TOL if P else True

        pe_exit = prow.px_exit_0920_next
        ce_exit = crow.px_exit_0920_next
        if pd.isna(pe_exit) or pd.isna(ce_exit):
            trades.append({"date": d, "status": "no_exit", "spot": spot,
                           "put_strike": int(prow.strike), "call_strike": int(crow.strike),
                           "pe_entry": P, "ce_entry": C, "net_points": None,
                           "prem_mismatch": flag})
            continue
        entry_credit = P + C
        exit_debit = float(pe_exit) + float(ce_exit)
        net = entry_credit - exit_debit
        trades.append({
            "date": d, "status": "ok", "expiry": prow.expiry, "dte": int(prow.dte),
            "spot": round(spot, 1), "put_strike": int(prow.strike), "call_strike": int(crow.strike),
            "put_otm": int(put_dist), "call_otm": int(crow.strike - spot),
            "pe_entry": round(P, 2), "ce_entry": round(C, 2),
            "pe_exit": round(float(pe_exit), 2), "ce_exit": round(float(ce_exit), 2),
            "entry_credit": round(entry_credit, 2), "exit_debit": round(exit_debit, 2),
            "net_points": round(net, 2), "prem_mismatch": flag})
    return pd.DataFrame(trades)


def metrics(t, lot, cost_pts):
    ok = t[t.status == "ok"].copy()
    if ok.empty:
        print("[METRICS] no completed trades."); return
    ok["net_after_cost"] = ok["net_points"] - cost_pts
    n = len(ok)
    wins = ok[ok.net_after_cost > 0]; losses = ok[ok.net_after_cost <= 0]
    win_rate = len(wins) / n * 100
    total = ok.net_after_cost.sum()
    avg = ok.net_after_cost.mean()
    pf = wins.net_after_cost.sum() / abs(losses.net_after_cost.sum()) if len(losses) and losses.net_after_cost.sum() != 0 else float("inf")
    # equity / drawdown (in points)
    eq = ok.net_after_cost.cumsum()
    dd = (eq - eq.cummax()).min()
    sharpe = (ok.net_after_cost.mean() / ok.net_after_cost.std() * math.sqrt(252)) if ok.net_after_cost.std() else 0
    # max consecutive losses
    mcl = cur = 0
    for v in ok.net_after_cost:
        cur = cur + 1 if v <= 0 else 0
        mcl = max(mcl, cur)

    print("\n" + "=" * 56)
    print("  WEEKLY SHORT STRANGLE — backtest (exact 5-min prices)")
    print("=" * 56)
    print(f"  period            : {ok.date.min()} -> {ok.date.max()}")
    print(f"  trades            : {n}   (no-exit/skipped: {len(t)-n})")
    print(f"  cost assumed      : {cost_pts} pts/round-trip   lot: {lot}")
    print(f"  win rate          : {win_rate:.1f}%")
    print(f"  total net         : {total:,.1f} pts   = Rs {total*lot:,.0f}")
    print(f"  avg / trade       : {avg:.2f} pts   = Rs {avg*lot:,.0f}")
    print(f"  avg win / loss    : {wins.net_after_cost.mean():.2f} / {losses.net_after_cost.mean():.2f} pts")
    print(f"  profit factor     : {pf:.2f}")
    print(f"  max drawdown      : {dd:,.1f} pts   = Rs {dd*lot:,.0f}")
    print(f"  max consec losses : {mcl}")
    print(f"  Sharpe (annual)   : {sharpe:.2f}")
    print(f"  premium-mismatch flagged trades: {int(ok.prem_mismatch.sum())} "
          f"({ok.prem_mismatch.mean()*100:.0f}%)")
    print("=" * 56)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lot", type=int, default=75)
    ap.add_argument("--cost-points", type=float, default=2.0)
    args = ap.parse_args()
    if not LEGS.exists():
        raise SystemExit(f"[FATAL] need {LEGS} (run upstox_fetcher.py)")
    df = pd.read_parquet(LEGS)
    t = build_trades(df)
    p = OUT / "strangle_trades.csv"
    t.to_csv(p, index=False)
    print(f"[TRADES] {len(t)} rows -> {p}")
    metrics(t, args.lot, args.cost_points)


if __name__ == "__main__":
    main()
