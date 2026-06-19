# Polymarket Multi-Asset Trading Bot

A real-time paper trading system for 5-minute binary prediction markets on [Polymarket](https://polymarket.com), built in Python using asyncio. Tracks Bitcoin (BTC), Ethereum (ETH), Solano (SOL), and Ripple (XRP) simultaneously with live WebSocket price feeds and a multi-factor signal engine.

---

## Overview

This bot monitors Polymarket's 5-minute "up/down" prediction markets for four crypto assets and makes paper trading decisions based on real-time order book probabilities filtered through trend, chop, and volatility analysis. All trades are logged to disk and never execute with real funds.

The system is designed around the idea that raw market probability alone is a noisy signal and that ideally, filters exist to suppress false positives from choppy, stagnant, or counter-trend conditions before a trade is opened.

---

## Architecture

```
Kraken WebSocket  ───────────────────────────────┐
(live price ticks for BTC/ETH/SOL/XRP)           │
                                                 ▼
Polymarket CLOB WebSocket  ──────────►  AssetState (per symbol)
(yes/no order book → P(up))                      │
                                                 ▼
                                        SignalEngine.evaluate()
                                                 │
                              ┌──────────────────┼──────────────────┐
                              ▼                  ▼                  ▼
                         TrendFilter        ChopFilter       VolatilityFilter
                         (prob history)     (DE + ZCR)       (ATR range gate)
                              │                  │                  │
                              └──────────────────┴──────────────────┘
                                                 │
                                                 ▼
                                        PaperTrader.open()
                                        PaperTrader.close()
                                                 │
                                                 ▼
                                         trades.json  ──────────►  (trade collection data only)
                                         live_accuracy.json
```

Two WebSocket connections run concurrently per asset:
- **Kraken** — live last-trade price used for P&L calculation and filter inputs
- **Polymarket CLOB** — real-time bid/ask on the YES token, used to estimate P(up)

---

## Signal Filters

### Trend Filter
Maintains a rolling window of recent P(up) values. If the market has been consistently bullish or bearish, the entry threshold for the opposing direction is raised. This prevents the algorithm from fighting a sustained directional move.

### Chop Filter
Combines two metrics on recent price ticks:
- **Directional Efficiency (DE):** net displacement / total path length
- **Zero-Crossing Rate (ZCR):** frequency of direction reversals

Low DE + high ZCR = choppy market → entry blocked.

### Volatility Gate
Measures the high-low range of BTC price ticks over a rolling window as a percentage of mean price. If the market is stagnant (range below threshold), all assets are blocked from entry. Weekend threshold is lower to account for reduced trading volume.

---

## Position Sizing

Position size is determined by confidence tier (derived from distance of P(up) from 0.5) and scaled by:
- Percentage of current balance (when `USE_PCT_SIZING` is enabled)
- A meta-filter multiplier based on historical win rates by UTC hour and day-of-week
- A short-side discount (`SHORT_SIZE_MULTIPLIER`) since short signals tend to be weaker

Certain confidence tiers can be blocked entirely via `BLOCKED_CONF_TIERS`.

---

## Exit Logic

Positions are closed when any of the following trigger:
- **Stop loss:** P&L drops below `-0.4%` of position size
- **Take profit:** P&L exceeds `+0.3%`
- **Stale scratch:** Position older than 40s with flat or negative P&L
- **Max hold:** Position open for 60+ seconds
- **Window expiring:** Less than 15 seconds left in the 5-minute market window

---

## Meta Filter

If a `meta_filter.json` file is present (generated from historical trade data), the bot uses it to:
- Block trading during historically worst UTC hours
- Scale position size up/down based on time-of-day and day-of-week win rates
- Skip confidence tiers with statistically poor historical performance

---

## Project Structure

```
├── bot.py                  # Main entry point — all logic in one file
├── trades.json             # Persistent trade log (auto-generated)
├── live_accuracy.json      # Running accuracy by hour, DOW, confidence tier
├── meta_filter.json        # Optional: historical performance overlays
└── bot.log                 # Runtime log with signal + trade events
```

---

## Setup

```bash
pip install httpx websockets
python bot.py
```

No API keys required. The bot reads public Polymarket and Kraken WebSocket feeds.

---

## Configuration

All parameters are in the `CONFIG` dict at the top of `bot.py`. Key settings:

| Parameter | Default | Description |
|---|---|---|
| `BUY_THRESHOLD` | 0.75 | Minimum P(up) to go long |
| `SELL_THRESHOLD` | 0.20 | Maximum P(up) to go short |
| `STOP_LOSS_PCT` | 0.4% | Exit if position loses this much |
| `TAKE_PROFIT_PCT` | 0.3% | Exit if position gains this much |
| `MAX_HOLD_SECONDS` | 60 | Force-close after this many seconds |
| `USE_PCT_SIZING` | True | Size as % of balance rather than fixed USD |
| `CHOP_THRESHOLD` | 0.60 | Minimum chop score to allow entry |
| `ATR_THRESHOLD` | 0.2% | Minimum BTC range to allow entry |

---

## Disclaimer

This is a paper trading system. No real funds are used or at risk. This project is for research and educational purposes only and does not constitute financial advice.
