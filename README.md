# Nifty weekly short strangle: a 72.4% win rate that still lost money

A backtest of an overnight short strangle on Nifty weekly options. The strategy wins about three trades in four and still ends down, because the average loss is roughly three times the average win.

Every figure below can be recomputed from `backtest_out/strangle_trades.csv`.

## Result

| Measure | Value |
| --- | --- |
| Period | 1 October 2024 to 19 May 2026 |
| Completed trades | 395 |
| Winning trades | 286 (72.4%) |
| Net result | -241.7 points, before costs |
| Average win / average loss | +7.6 / -22.3 points |
| Worst single trade | -457.7 points |
| Friday entries (77 trades) | -770.5 points |
| All other weekdays | +528.8 points |
| Put leg / call leg | -295.8 / +54.1 points |

One weekday and one leg explain the whole result. Entries on a Friday carry the weekend gap, and the put side carries the downside gaps. Without Fridays the strategy is positive; with them it is not.

## The rule

- Expiry: the weekly contract with at least 7 days to expiry.
- Put: strike closest to spot minus 750 points.
- Call: at least 600 points out of the money and within 150 points of the put's distance, choosing the strike whose premium is closest to the put's.
- Sell both at 15:15, buy both back at 09:20 the next trading day.

## Files

| File | What it is |
| --- | --- |
| `strangle_backtest.py` | Builds the trades from the option legs and prints the summary |
| `backtest_out/strangle_trades.csv` | The trade log: one row per day, with strikes, entry and exit prices and net points |
| `dhan_fetcher.py` | Data pipeline for minute-level option history. Missing data is logged as a gap and never filled with a model price |

## What is not here

- Raw market data. It is licensed from the data vendors and is not redistributed.
- API keys. The fetcher reads them from environment variables.

## Limits

- Prices are 5-minute bars at 15:15 and 09:20, not executed fills. Slippage is not modelled; the script takes a cost per round trip as an input (default 2 points), and the table above is before that cost.
- One instrument and about 20 months of data. This is evidence about this rule in this period, not a general claim about short strangles.

Not investment advice.
