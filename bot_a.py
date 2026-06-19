from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
import websockets


# ─── PATHS ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(".")
BOT_LOG_PATH = BASE_DIR / "bot.log"
TRADES_PATH = BASE_DIR / "trades.json"
LIVE_ACCURACY_PATH = BASE_DIR / "live_accuracy.json"
META_FILTER_PATH = BASE_DIR / "meta_filter.json"


# ─── CONFIG ───────────────────────────────────────────────────────────────────

CONFIG = {
    "BUY_THRESHOLD": 0.75,
    "SELL_THRESHOLD": 0.20,
    "SIZE_TIERS": [
        (0.95, 1000),
        (0.80, 750),
        (0.70, 500),
        (0.60, 300),
    ],
    "USD_BAND_OVERRIDES": {
        "95-97%": 1000,
        "97-100%": 750,
    },
    "USE_PCT_SIZING": True,
    "PCT_TIERS": [
        (0.95, 0.10),
        (0.80, 0.075),
        (0.70, 0.05),
        (0.60, 0.03),
    ],
    "PCT_BAND_OVERRIDES": {
        "95-97%": 0.10,
        "97-100%": 0.075,
    },
    "BLOCKED_CONF_TIERS": ["97-100%"],
    "MAX_DRAWDOWN_PCT": 0.15,
    "STOP_LOSS_PCT": 0.004,
    "TAKE_PROFIT_PCT": 0.003,
    "MAX_HOLD_SECONDS": 60,
    "MIN_WINDOW_SECONDS_LEFT": 90,
    "SIGNAL_DEBOUNCE_MS": 100,
    "MIN_SIGNAL_STRENGTH": 0.03,
    "MAX_PROB_AGE_SECONDS": 8,
    "SHORT_SIZE_MULTIPLIER": 0.60,
    "SCRATCH_SECONDS": 40,
    "SCRATCH_MAX_PNL_PCT": 0.0,
    # Chop filter
    "CHOP_WINDOW": 20,
    "CHOP_MIN_TICKS": 8,
    "CHOP_THRESHOLD": 0.60,
    "CHOP_DE_WEIGHT": 0.70,
    # Trend filter
    "TREND_WINDOW": 40,
    "TREND_MIN_SAMPLES": 12,
    "TREND_GATE_FACTOR": 0.70,
    # Volatility gate
    "ATR_WINDOW": 60,
    "ATR_MIN_TICKS": 40,
    "ATR_THRESHOLD": 0.002,
    "ATR_THRESHOLD_WE": 0.0013,
    # Meta filter
    "META_FILTER_THRESHOLD": 0.0,
    "META_SIZE_SCALE": 0.3,
    "BLOCK_WORST_HOURS": True,
    "WORST_HOURS_UTC": [1, 2],
    # Endpoints
    "GAMMA_API": "https://gamma-api.polymarket.com",
    "CLOB_WS": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "KRAKEN_WS_URL": "wss://ws.kraken.com/v2",
}

ASSETS = {
    "BTC": {"slug_prefix": "btc-updown-5m", "kraken_symbol": "BTC/USD"},
    "ETH": {"slug_prefix": "eth-updown-5m", "kraken_symbol": "ETH/USD"},
    "SOL": {"slug_prefix": "sol-updown-5m", "kraken_symbol": "SOL/USD"},
    "XRP": {"slug_prefix": "xrp-updown-5m", "kraken_symbol": "XRP/USD"},
}


# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(BOT_LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger("bot")


# ─── UTILS ────────────────────────────────────────────────────────────────────


def utc_now() -> datetime:
    return datetime.now(timezone.utc)



def load_json_file(path: Path, default):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)



def save_json_file(path: Path, payload) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)



def current_5min_ts() -> int:
    now = int(time.time())
    return now - (now % 300)



def next_5min_ts() -> int:
    return current_5min_ts() + 300



def secs_until_next_window() -> float:
    return next_5min_ts() - time.time()



def confidence_label(confidence: float) -> str:
    if confidence >= 0.97:
        return "97-100%"
    if confidence >= 0.95:
        return "95-97%"
    if confidence >= 0.90:
        return "90-95%"
    if confidence >= 0.80:
        return "80-90%"
    if confidence >= 0.70:
        return "70-80%"
    return "60-70%"


# ─── META FILTER ──────────────────────────────────────────────────────────────

META_FILTER: dict = {}
SKIP_CONFIDENCE_TIERS: list[str] = []
WORST_HOURS_UTC: list[int] = CONFIG["WORST_HOURS_UTC"][:]



def load_meta_filter(path: Path = META_FILTER_PATH) -> None:
    """
    Load optional historical context filters.

    The current test configuration keeps worst-hour blocking explicit in CONFIG,
    while historical context remains optional and informational.
    """
    global META_FILTER, SKIP_CONFIDENCE_TIERS, WORST_HOURS_UTC

    WORST_HOURS_UTC = CONFIG["WORST_HOURS_UTC"][:]

    if not path.exists():
        META_FILTER = {}
        SKIP_CONFIDENCE_TIERS = []
        log.info("No meta_filter.json found. Running without historical filter.")
        return

    META_FILTER = load_json_file(path, {})
    SKIP_CONFIDENCE_TIERS = META_FILTER.get("skip_confidence_tiers", [])

    log.info(f"Meta-filter loaded from {path}")
    log.info(f"   Assets analyzed     : {META_FILTER.get('assets_analyzed', [])}")
    log.info(f"   Best hours (UTC)    : {META_FILTER.get('best_hours_utc', [])}")
    log.info(f"   Config worst hours  : {WORST_HOURS_UTC}")
    log.info(f"   Hist worst hours    : {META_FILTER.get('worst_hours_utc', [])}")
    log.info(f"   Skip conf. tiers    : {SKIP_CONFIDENCE_TIERS}")
    _print_hourly_table()



def _print_hourly_table() -> None:
    hourly = META_FILTER.get("hourly_accuracy", {})
    if not hourly:
        return

    rows = [(int(hour), data) for hour, data in hourly.items() if data.get("n", 0) >= 10]
    rows.sort(key=lambda item: item[1]["accuracy"], reverse=True)

    log.info("\n  ── Historical Accuracy by UTC Hour ──")
    log.info(f"  {'Hour':>6}  {'Accuracy':>8}  {'N':>5}  {'Z':>6}  {'Signal'}")
    log.info(f"  {'─' * 45}")

    for hour, data in rows:
        accuracy = data["accuracy"]
        z_score = data.get("z", 0)
        bar = "|||" * int(accuracy * 20)
        tag = "BEST" if hour in META_FILTER.get("best_hours_utc", []) else ""
        if not tag and hour in WORST_HOURS_UTC:
            tag = "WORST"
        log.info(f"  {hour:02d}:00    {accuracy:7.1%}  {data['n']:5}  {z_score:+5.2f}  {bar} {tag}")



def meta_score(hour: int, dow: int, consecutive_same: int) -> float:
    if not META_FILTER:
        return 0.5

    score = 0.5

    hourly = META_FILTER.get("hourly_accuracy", {})
    hour_data = hourly.get(str(hour), {})
    if hour_data.get("n", 0) >= 20:
        score += (hour_data["accuracy"] - 0.5) * 0.50

    dow_data = META_FILTER.get("dow_accuracy", {})
    day_data = dow_data.get(str(dow), {})
    if day_data.get("n", 0) >= 20:
        score += (day_data["accuracy"] - 0.5) * 0.30

    streak_data = META_FILTER.get("consecutive_bonus", {})
    streak_key = str(min(consecutive_same, 5))
    if streak_key in streak_data:
        score += streak_data[streak_key] * 0.20

    return max(0.0, min(1.0, score))



def meta_size_multiplier(score: float) -> float:
    scale = CONFIG["META_SIZE_SCALE"]
    if scale == 0:
        return 1.0
    mult = 1.0 + scale * (score - 0.5) / 0.5
    return max(0.5, min(1.5, mult))


# ─── MARKET DISCOVERY ─────────────────────────────────────────────────────────

async def fetch_market(slug_prefix: str) -> Optional[dict]:
    """Find the active Polymarket 5-minute market for a given asset."""
    async with httpx.AsyncClient(timeout=15) as client:
        for offset in [0, 1, -1, 2]:
            ts = current_5min_ts() + (offset * 300)
            slug = f"{slug_prefix}-{ts}"
            try:
                url = f"{CONFIG['GAMMA_API']}/events?slug={slug}&limit=1"
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json()
                if not data:
                    continue

                event = data[0] if isinstance(data, list) else data
                markets = event.get("markets", [])
                if not markets:
                    continue

                up_market = next(
                    (
                        market
                        for market in markets
                        if "up" in market.get("question", "").lower()
                        or "higher" in market.get("question", "").lower()
                    ),
                    None,
                )
                if up_market is None:
                    log.warning(f"Could not find UP market in {slug} — skipping")
                    continue

                condition_id = up_market.get("conditionId") or up_market.get("condition_id", "")
                raw_ids = up_market.get("clobTokenIds", "[]")
                token_ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
                if not token_ids:
                    continue

                return {
                    "slug": slug,
                    "condition_id": condition_id,
                    "yes_token_id": token_ids[0],
                    "end_time": ts + 300,
                }
            except Exception as exc:
                log.debug(f"Market lookup failed ({slug}): {exc}")
                continue
    return None


# ─── POSITION SIZING ──────────────────────────────────────────────────────────


def get_position_size(
    prob: float,
    score: float = 0.5,
    balance: float = 0.0,
    direction: str = "long",
) -> float:
    confidence = max(prob, 1 - prob)
    tier_label = confidence_label(confidence)

    blocked_tiers = CONFIG.get("BLOCKED_CONF_TIERS", [])
    dynamic_blocked = SKIP_CONFIDENCE_TIERS or []
    blocked_union = set(blocked_tiers) | set(dynamic_blocked)
    if tier_label in blocked_union:
        log.debug(f"  {tier_label} tier blocked ({confidence:.0%})")
        return 0

    if CONFIG.get("USE_PCT_SIZING", False) and balance > 0:
        pct_overrides = CONFIG.get("PCT_BAND_OVERRIDES", {})
        if tier_label in pct_overrides:
            base = round(balance * pct_overrides[tier_label] / 50) * 50
        else:
            base = 0
            for threshold, pct in CONFIG["PCT_TIERS"]:
                if confidence >= threshold:
                    base = round(balance * pct / 50) * 50
                    break
            if base == 0:
                base = round(balance * 0.03 / 50) * 50
    else:
        usd_overrides = CONFIG.get("USD_BAND_OVERRIDES", {})
        if tier_label in usd_overrides:
            base = usd_overrides[tier_label]
        else:
            base = 300
            for threshold, size in CONFIG["SIZE_TIERS"]:
                if confidence >= threshold:
                    base = size
                    break

    if direction == "short":
        base = round(base * CONFIG.get("SHORT_SIZE_MULTIPLIER", 1.0) / 50) * 50

    final = round(base * meta_size_multiplier(score) / 50) * 50
    return max(150, min(final, 1500))


# ─── DATA MODELS ──────────────────────────────────────────────────────────────

@dataclass
class AssetState:
    symbol: str
    price: float = 0.0
    price_ts: float = 0.0
    prob_up: float = 0.5
    prob_ts: float = 0.0
    current_market: Optional[dict] = None
    market_expires_at: float = 0.0
    consecutive_same_dir: int = 0
    last_signal_dir: str = ""
    prob_history: deque = field(default_factory=lambda: deque(maxlen=CONFIG["TREND_WINDOW"]))
    price_ticks: deque = field(default_factory=lambda: deque(maxlen=CONFIG["CHOP_WINDOW"]))
    price_ticks_atr: deque = field(default_factory=lambda: deque(maxlen=CONFIG["ATR_WINDOW"]))

    @property
    def prob_fresh(self) -> bool:
        return (time.time() - self.prob_ts) < CONFIG["MAX_PROB_AGE_SECONDS"]

    @property
    def secs_left(self) -> float:
        return max(0.0, self.market_expires_at - time.time())

    @property
    def ok_to_enter(self) -> bool:
        return self.secs_left >= CONFIG["MIN_WINDOW_SECONDS_LEFT"]


@dataclass
class Position:
    symbol: str
    direction: str
    entry_price: float
    size_usd: float
    entry_time: float = field(default_factory=time.time)
    entry_prob: float = 0.5
    meta_score: float = 0.5
    hour_utc: int = 0
    dow: int = 0
    trend_bias: float = 0.5
    trend_gate: float = 0.0
    chop_score: float = 0.5

    @property
    def age(self) -> float:
        return time.time() - self.entry_time

    def pnl(self, price: float) -> float:
        if self.direction == "long":
            return self.size_usd * (price - self.entry_price) / self.entry_price
        return self.size_usd * (self.entry_price - price) / self.entry_price

    def pnl_pct(self, price: float) -> float:
        if self.entry_price == 0:
            return 0.0
        if self.direction == "long":
            return (price - self.entry_price) / self.entry_price
        return (self.entry_price - price) / self.entry_price


# ─── PAPER TRADER ─────────────────────────────────────────────────────────────

class PaperTrader:
    def __init__(self):
        trades = load_json_file(TRADES_PATH, [])

        self.positions: dict[str, Position] = {}
        self.trade_log: list = trades
        self.trade_count: int = len(trades)
        self.wins: int = sum(1 for trade in trades if float(trade.get("pnl", 0)) > 0)
        self.losses: int = sum(1 for trade in trades if float(trade.get("pnl", 0)) <= 0)
        self.total_pnl: float = sum(float(trade.get("pnl", 0)) for trade in trades)
        self.balance: float = 10_000.0 + self.total_pnl
        self._peak_balance: float = self.balance

        self.hourly_live: dict = {}
        self.dow_live: dict = {}
        self.conf_live: dict = {}

    def has_position(self, symbol: str) -> bool:
        return symbol in self.positions

    def open(
        self,
        symbol: str,
        direction: str,
        price: float,
        prob: float,
        score: float = 0.5,
        hour: int = 0,
        dow: int = 0,
        trend_bias: float = 0.5,
        trend_gate: float = 0.0,
        chop_score: float = 0.5,
    ) -> None:
        if self.has_position(symbol):
            return

        size = get_position_size(prob, score, self.balance, direction)
        if size == 0:
            return

        if CONFIG["BLOCK_WORST_HOURS"] and WORST_HOURS_UTC and hour in WORST_HOURS_UTC:
            log.info(f"  [{symbol}] Blocked — hour {hour:02d}:00 UTC is historically worst")
            return

        max_dd = CONFIG.get("MAX_DRAWDOWN_PCT", 0.0)
        if max_dd > 0:
            self._peak_balance = max(self._peak_balance, self.balance)
            if self.balance < self._peak_balance * (1 - max_dd):
                drawdown_pct = (self._peak_balance - self.balance) / self._peak_balance * 100
                log.warning(
                    f"  [{symbol}] DRAWDOWN GUARD: balance ${self.balance:.2f} is "
                    f"{drawdown_pct:.1f}% below peak ${self._peak_balance:.2f} — pausing entries"
                )
                return

        threshold = CONFIG["META_FILTER_THRESHOLD"]
        if threshold > 0 and score < threshold:
            log.info(f"  [{symbol}] Blocked by meta-filter (score {score:.3f} < {threshold:.2f})")
            return

        position = Position(
            symbol=symbol,
            direction=direction,
            entry_price=price,
            size_usd=size,
            entry_prob=prob,
            meta_score=score,
            hour_utc=hour,
            dow=dow,
            trend_bias=trend_bias,
            trend_gate=trend_gate,
            chop_score=chop_score,
        )
        self.positions[symbol] = position

        tier = confidence_label(max(prob, 1 - prob))
        gate_str = f" gate={trend_gate:+.2f}" if abs(trend_gate) > 0.01 else ""
        chop_str = f" chop={chop_score:.2f}" if chop_score < 0.8 else ""

        log.info(
            f"Positively trending - [{symbol}] OPEN {direction.upper()} | "
            f"${price:,.4g} | P(up):{prob:.0%} | Conf:{tier} | "
            f"Size:${size:,} (meta×{meta_size_multiplier(score):.2f}) | Score:{score:.3f} | "
            f"Bias:{trend_bias:.2f}{gate_str}{chop_str} | {hour:02d}:00 UTC"
        )

    def check_exit(self, symbol: str, price: float, state: AssetState) -> Optional[str]:
        position = self.positions.get(symbol)
        if not position:
            return None

        pct = position.pnl_pct(price)
        if pct <= -CONFIG["STOP_LOSS_PCT"]:
            return "stop_loss"
        if pct >= CONFIG["TAKE_PROFIT_PCT"]:
            return "take_profit"
        if (
            position.age >= CONFIG.get("SCRATCH_SECONDS", 0)
            and pct <= CONFIG.get("SCRATCH_MAX_PNL_PCT", 0.0)
        ):
            return "stale_scratch"
        if position.age >= CONFIG["MAX_HOLD_SECONDS"]:
            return "timeout"
        if state.market_expires_at > 0 and state.secs_left < 15:
            return "window_expiring"
        return None

    def close(self, symbol: str, price: float, reason: str) -> None:
        position = self.positions.pop(symbol, None)
        if not position:
            return

        pnl = position.pnl(price)
        pct = position.pnl_pct(price)
        self.total_pnl += pnl
        self.balance += pnl
        self.trade_count += 1

        won = pnl > 0
        if won:
            self.wins += 1
            response = "Optimal"
        else:
            self.losses += 1
            response = "Suboptimal"

        self._tally(self.hourly_live, str(position.hour_utc), won)
        self._tally(self.dow_live, str(position.dow), won)
        self._tally(self.conf_live, confidence_label(max(position.entry_prob, 1 - position.entry_prob)), won)

        log.info(
            f"{response} [{symbol}] CLOSE {position.direction.upper()} | "
            f"Reason:{reason} | PnL:${pnl:+.2f} ({pct:+.3%}) | "
            f"Score:{position.meta_score:.3f} | Balance:${self.balance:,.2f}"
        )

        self.trade_log.append(
            {
                "time": utc_now().isoformat(),
                "symbol": symbol,
                "direction": position.direction,
                "entry_price": position.entry_price,
                "exit_price": price,
                "size_usd": position.size_usd,
                "pnl": round(pnl, 4),
                "pnl_pct": round(pct, 6),
                "reason": reason,
                "hold_seconds": round(position.age, 1),
                "entry_prob": position.entry_prob,
                "meta_score": round(position.meta_score, 4),
                "hour_utc": position.hour_utc,
                "dow": position.dow,
                "conf_tier": confidence_label(max(position.entry_prob, 1 - position.entry_prob)),
                "trend_bias": round(position.trend_bias, 4),
                "trend_gate": round(position.trend_gate, 4),
                "chop_score": round(position.chop_score, 4),
            }
        )

    @staticmethod
    def _tally(store: dict, key: str, won: bool) -> None:
        if key not in store:
            store[key] = {"n": 0, "wins": 0}
        store[key]["n"] += 1
        store[key]["wins"] += int(won)

    def force_close_symbol(self, symbol: str, price: float) -> None:
        if self.has_position(symbol) and price > 0:
            self.close(symbol, price, "window_rollover")

    def stats(self) -> str:
        win_rate = self.wins / self.trade_count if self.trade_count else 0
        open_positions = list(self.positions.keys()) or ["none"]
        return (
            f"Trades:{self.trade_count} | WR:{win_rate:.0%} | "
            f"PnL:${self.total_pnl:+.2f} | Balance:${self.balance:,.2f} | "
            f"Open:{', '.join(open_positions)}"
        )

    def live_vs_historical_summary(self) -> str:
        if not META_FILTER or not self.hourly_live:
            return ""

        lines = ["  ── Live vs Historical Accuracy ──"]
        historical_hourly = META_FILTER.get("hourly_accuracy", {})

        for hour_str, live in sorted(self.hourly_live.items(), key=lambda item: int(item[0])):
            if live["n"] < 3:
                continue
            live_acc = live["wins"] / live["n"]
            hist = historical_hourly.get(hour_str, {})
            hist_acc = hist.get("accuracy", 0.5)
            diff = live_acc - hist_acc
            flag = "^^^" if diff > 0.05 else "\/\/\/" if diff < -0.05 else "→"
            lines.append(
                f"  {int(hour_str):02d}:00 UTC  live:{live_acc:.0%}(n={live['n']:2})  "
                f"hist:{hist_acc:.0%}  diff:{diff:+.0%} {flag}"
            )

        return "\n".join(lines) if len(lines) > 1 else ""


# ─── TREND FILTER ─────────────────────────────────────────────────────────────

class TrendFilter:
    def __init__(self):
        self._factor = CONFIG["TREND_GATE_FACTOR"]
        self._min = CONFIG["TREND_MIN_SAMPLES"]

    def bias(self, state: AssetState) -> float:
        history = state.prob_history
        if len(history) < self._min:
            return 0.5
        return sum(history) / len(history)

    def strength(self, state: AssetState) -> float:
        return abs(self.bias(state) - 0.5)

    def samples(self, state: AssetState) -> int:
        return len(state.prob_history)

    def effective_buy(self, state: AssetState) -> float:
        bias = self.bias(state)
        penalty = max(0.0, 0.5 - bias) * self._factor
        return min(CONFIG["BUY_THRESHOLD"] + penalty, 0.97)

    def effective_sell(self, state: AssetState) -> float:
        bias = self.bias(state)
        penalty = max(0.0, bias - 0.5) * self._factor
        return max(CONFIG["SELL_THRESHOLD"] - penalty, 0.03)

    def describe(self, state: AssetState) -> str:
        history = state.prob_history
        if len(history) < self._min:
            return f"trend:warm-up({len(history)}/{self._min})"

        bias = self.bias(state)
        strength = self.strength(state)
        buy = self.effective_buy(state)
        sell = self.effective_sell(state)
        direction = "bearish" if bias < 0.48 else "bullish" if bias > 0.52 else "neutral"
        return (
            f"trend:{direction} bias={bias:.2f} str={strength:.2f} "
            f"→ L>{buy:.2f} S<{sell:.2f} (n={len(history)})"
        )

    def trend_label(self, state: AssetState) -> str:
        if len(state.prob_history) < self._min:
            return "~"
        bias = self.bias(state)
        strength = self.strength(state)
        if strength < 0.05:
            return "neutral"
        if bias < 0.5:
            return f"▼{bias:.2f}" if strength < 0.15 else f"▼▼{bias:.2f}"
        return f"▲{bias:.2f}" if strength < 0.15 else f"▲▲{bias:.2f}"


TREND = TrendFilter()


# ─── CHOP FILTER ──────────────────────────────────────────────────────────────

class ChopFilter:
    def __init__(self):
        self._threshold = CONFIG["CHOP_THRESHOLD"]
        self._min_ticks = CONFIG["CHOP_MIN_TICKS"]
        self._de_weight = CONFIG["CHOP_DE_WEIGHT"]
        self._zcr_weight = 1.0 - CONFIG["CHOP_DE_WEIGHT"]

    def _de(self, ticks: list[float]) -> float:
        if len(ticks) < 2:
            return 0.5
        moves = [ticks[i + 1] - ticks[i] for i in range(len(ticks) - 1)]
        total_path = sum(abs(move) for move in moves)
        if total_path == 0:
            return 0.5
        net = abs(ticks[-1] - ticks[0])
        return net / total_path

    def _zcr(self, ticks: list[float]) -> float:
        if len(ticks) < 3:
            return 0.5
        moves = [ticks[i + 1] - ticks[i] for i in range(len(ticks) - 1)]
        non_zero = [move for move in moves if move != 0]
        if len(non_zero) < 2:
            return 0.0
        crossings = sum(
            1 for i in range(len(non_zero) - 1) if (non_zero[i] > 0) != (non_zero[i + 1] > 0)
        )
        return crossings / (len(non_zero) - 1)

    def score(self, state: AssetState) -> float:
        ticks = list(state.price_ticks)
        if len(ticks) < self._min_ticks:
            return 0.5
        de = self._de(ticks)
        zcr = self._zcr(ticks)
        return de * self._de_weight + (1.0 - zcr) * self._zcr_weight

    def is_trending(self, state: AssetState) -> bool:
        if self._threshold == 0:
            return True
        if len(state.price_ticks) < self._min_ticks:
            return True
        return self.score(state) >= self._threshold

    def label(self, state: AssetState) -> str:
        if len(state.price_ticks) < self._min_ticks:
            return f"chop:~({len(state.price_ticks)}/{self._min_ticks})"
        score = self.score(state)
        if score >= 0.60:
            bar = "▰▰▰▰"
        elif score >= 0.45:
            bar = "▰▰▰░"
        elif score >= 0.30:
            bar = "▰▰░░"
        else:
            bar = "▰░░░"
        verdict = "OK" if score >= self._threshold else "CHOP"
        return f"chop:{score:.2f}{bar} {verdict}"


CHOP = ChopFilter()


# ─── VOLATILITY GATE ──────────────────────────────────────────────────────────

class VolatilityFilter:
    def __init__(self):
        self._min_ticks = CONFIG["ATR_MIN_TICKS"]

    def range_pct(self, state: AssetState) -> float:
        ticks = list(state.price_ticks_atr)
        if len(ticks) < self._min_ticks or min(ticks) == 0:
            return 1.0
        mean_price = sum(ticks) / len(ticks)
        if mean_price == 0:
            return 1.0
        return (max(ticks) - min(ticks)) / mean_price

    def current_threshold(self) -> float:
        dow = utc_now().weekday()
        if dow >= 5:
            return CONFIG.get("ATR_THRESHOLD_WE", CONFIG["ATR_THRESHOLD"])
        return CONFIG["ATR_THRESHOLD"]

    def is_active(self, btc_state: AssetState) -> bool:
        if CONFIG["ATR_THRESHOLD"] == 0:
            return True
        return self.range_pct(btc_state) >= self.current_threshold()

    def label(self, state: AssetState, btc_state: AssetState) -> str:
        rng = self.range_pct(state)
        btc_rng = self.range_pct(btc_state)
        ticks = len(state.price_ticks_atr)
        if ticks < self._min_ticks:
            return f"vol:warm({ticks}/{self._min_ticks})"
        threshold = self.current_threshold()
        verdict = "OK" if btc_rng >= threshold else "STAGNANT"
        tag = "WE" if utc_now().weekday() >= 5 else "WD"
        return f"vol:{rng*100:.3f}% t={threshold*100:.2f}% [{tag}] {verdict}"


VOL = VolatilityFilter()


# ─── SIGNAL ENGINE ────────────────────────────────────────────────────────────

class SignalEngine:
    def __init__(self, trader: PaperTrader, states: dict[str, AssetState]):
        self.trader = trader
        self._states = states
        self._last_prob: dict[str, float] = {}
        self._last_trade_time: dict[str, float] = {}

    def evaluate(self, state: AssetState) -> None:
        symbol = state.symbol
        now = time.time()

        if state.price == 0 or (now - state.price_ts) > 5:
            return

        if self.trader.has_position(symbol):
            reason = self.trader.check_exit(symbol, state.price, state)
            if reason:
                self.trader.close(symbol, state.price, reason)
                log.info(f"LOGGED: {self.trader.stats()}")
            return

        if not state.prob_fresh:
            return
        if (now - self._last_trade_time.get(symbol, 0)) < CONFIG["SIGNAL_DEBOUNCE_MS"] / 1000:
            return
        if not state.ok_to_enter:
            return

        btc_state = self._states.get("BTC", state)
        if not VOL.is_active(btc_state):
            threshold = VOL.current_threshold()
            btc_range = VOL.range_pct(btc_state) * 100
            log.debug(
                f"  [{symbol}] Stagnation gate blocked "
                f"(BTC range={btc_range:.3f}% < {threshold*100:.2f}% thresh)"
            )
            return

        chop_score = CHOP.score(state)
        if not CHOP.is_trending(state):
            log.debug(
                f"  [{symbol}] Chop gate: entry blocked "
                f"(score={chop_score:.3f} < threshold={CONFIG['CHOP_THRESHOLD']:.2f}, {CHOP.label(state)})"
            )
            return

        prob = state.prob_up
        last_prob = self._last_prob.get(symbol, 0.5)
        first_ever = symbol not in self._last_trade_time

        now_dt = utc_now()
        score = meta_score(
            hour=now_dt.hour,
            dow=now_dt.weekday(),
            consecutive_same=state.consecutive_same_dir,
        )

        buy_threshold = TREND.effective_buy(state)
        sell_threshold = TREND.effective_sell(state)

        if prob > buy_threshold:
            if first_ever or (prob - last_prob) >= CONFIG["MIN_SIGNAL_STRENGTH"]:
                base_buy = CONFIG["BUY_THRESHOLD"]
                if buy_threshold > base_buy + 0.01:
                    log.info(
                        f"  [{symbol}] Trend gate active: long needs >{buy_threshold:.2f} "
                        f"(base {base_buy:.2f}, {TREND.describe(state)})"
                    )
                self.trader.open(
                    symbol,
                    "long",
                    state.price,
                    prob,
                    score,
                    hour=now_dt.hour,
                    dow=now_dt.weekday(),
                    trend_bias=TREND.bias(state),
                    trend_gate=round(buy_threshold - CONFIG["BUY_THRESHOLD"], 3),
                    chop_score=round(chop_score, 4),
                )
                self._update_streak(state, "long")
                self._last_prob[symbol] = prob
                self._last_trade_time[symbol] = now

        elif prob < sell_threshold:
            if first_ever or (last_prob - prob) >= CONFIG["MIN_SIGNAL_STRENGTH"]:
                base_sell = CONFIG["SELL_THRESHOLD"]
                if sell_threshold < base_sell - 0.01:
                    log.info(
                        f"  [{symbol}] Trend gate active: short needs <{sell_threshold:.2f} "
                        f"(base {base_sell:.2f}, {TREND.describe(state)})"
                    )
                self.trader.open(
                    symbol,
                    "short",
                    state.price,
                    prob,
                    score,
                    hour=now_dt.hour,
                    dow=now_dt.weekday(),
                    trend_bias=TREND.bias(state),
                    trend_gate=round(CONFIG["SELL_THRESHOLD"] - sell_threshold, 3),
                    chop_score=round(chop_score, 4),
                )
                self._update_streak(state, "short")
                self._last_prob[symbol] = prob
                self._last_trade_time[symbol] = now

        else:
            if buy_threshold > CONFIG["BUY_THRESHOLD"] + 0.02 and prob > CONFIG["BUY_THRESHOLD"]:
                log.debug(f"  [{symbol}] Long blocked by trend gate (P={prob:.2f} < required {buy_threshold:.2f})")
            elif sell_threshold < CONFIG["SELL_THRESHOLD"] - 0.02 and prob < CONFIG["SELL_THRESHOLD"]:
                log.debug(f"  [{symbol}] Short blocked by trend gate (P={prob:.2f} > required {sell_threshold:.2f})")

    @staticmethod
    def _update_streak(state: AssetState, direction: str) -> None:
        if direction == state.last_signal_dir:
            state.consecutive_same_dir += 1
        else:
            state.consecutive_same_dir = 1
        state.last_signal_dir = direction


# ─── PRICE FEEDS ──────────────────────────────────────────────────────────────

async def kraken_feed(states: dict[str, AssetState]) -> None:
    symbols = [info["kraken_symbol"] for info in ASSETS.values()]
    subscribe = json.dumps({
        "method": "subscribe",
        "params": {"channel": "ticker", "symbol": symbols},
    })
    kraken_to_symbol = {info["kraken_symbol"]: symbol for symbol, info in ASSETS.items()}

    while True:
        try:
            log.info("Connecting to Kraken...")
            async with websockets.connect(CONFIG["KRAKEN_WS_URL"], ping_interval=20) as ws: #perhaps turn into interval pinger but likely is inefficient
                await ws.send(subscribe)
                log.info(f"Kraken is live --- {', '.join(ASSETS.keys())}")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get("channel") == "ticker" and msg.get("type") in ("snapshot", "update"):
                            for item in msg.get("data", []):
                                kraken_symbol = item.get("symbol")
                                last_price = item.get("last")
                                if kraken_symbol and last_price and kraken_symbol in kraken_to_symbol:
                                    symbol = kraken_to_symbol[kraken_symbol]
                                    price = float(last_price)
                                    states[symbol].price = price
                                    states[symbol].price_ts = time.time()
                                    states[symbol].price_ticks.append(price)
                                    states[symbol].price_ticks_atr.append(price)
                    except Exception:
                        pass
        except Exception as exc:
            log.warning(f"Kraken error: {exc} --- reconnecting in 3s")
            await asyncio.sleep(3)



def _parse_book(msg: dict, state: AssetState, yes_token: str) -> None:
    event_type = msg.get("event_type") or msg.get("type", "")
    asset_id = msg.get("asset_id", "")
    if asset_id and asset_id != yes_token:
        return

    if event_type in ("book", "price_change"):
        buys = msg.get("buys", [])
        sells = msg.get("sells", [])
        if buys and sells:
            try:
                bid = float(buys[0]["price"])
                ask = float(sells[0]["price"])
                if 0 < bid < ask <= 1:
                    state.prob_up = (bid + ask) / 2
                    state.prob_ts = time.time()
                    state.prob_history.append(state.prob_up)
            except (KeyError, ValueError, IndexError):
                pass
    elif event_type == "last_trade_price" and not state.prob_fresh:
        try:
            price = float(msg.get("price", 0))
            if 0 < price <= 1:
                state.prob_up = price
                state.prob_ts = time.time()
                state.prob_history.append(state.prob_up)
        except ValueError:
            pass


async def asset_feed(symbol: str, state: AssetState, trader: PaperTrader) -> None:
    slug_prefix = ASSETS[symbol]["slug_prefix"]

    while True:
        market = await fetch_market(slug_prefix)
        if not market:
            log.warning(f"[{symbol}] No market found — retrying in 30s")
            await asyncio.sleep(30)
            continue

        state.current_market = market
        state.market_expires_at = float(market["end_time"])
        yes_token = market["yes_token_id"]

        trader.force_close_symbol(symbol, state.price)
        state.prob_up = 0.5
        state.prob_ts = 0.0

        log.info(f"[{symbol}] Market: {market['slug']} | {state.secs_left:.0f}s left")

        subscribe = json.dumps({
            "auth": {},
            "type": "subscribe",
            "assets_ids": [yes_token],
        })
        window_deadline = state.market_expires_at + 5

        while time.time() < window_deadline:
            try:
                async with websockets.connect(CONFIG["CLOB_WS"], ping_interval=15, close_timeout=5) as ws:
                    await ws.send(subscribe)
                    log.info(f"[{symbol}] Polymarket WS live")
                    async for raw in ws:
                        if time.time() >= window_deadline:
                            break
                        try:
                            payload = json.loads(raw)
                            messages = payload if isinstance(payload, list) else [payload]
                            for msg in messages:
                                _parse_book(msg, state, yes_token)
                        except Exception:
                            pass
            except Exception as exc:
                if time.time() < window_deadline:
                    log.warning(f"[{symbol}] WS dropped: {exc} --- reconnecting in 2s")
                    await asyncio.sleep(2)
                else:
                    break

        wait = secs_until_next_window()
        if wait > 0:
            log.info(f"[{symbol}] Window ended --- next in {wait:.0f}s")
            await asyncio.sleep(wait + 3)


# ─── LOOPS ────────────────────────────────────────────────────────────────────

async def signal_loop(states: dict[str, AssetState], engine: SignalEngine) -> None:
    while True:
        for state in states.values():
            engine.evaluate(state)
        await asyncio.sleep(0.1)


async def status_loop(states: dict[str, AssetState], trader: PaperTrader) -> None:
    tick = 0
    while True:
        await asyncio.sleep(300)  # every 5 minutes instead of 5s; google cloud compute optimization method
        tick += 1
        lines = [f"\n{'─' * 70}"]
        now_dt = utc_now()
        hour = now_dt.hour

        hourly_hist = META_FILTER.get("hourly_accuracy", {})
        hour_data = hourly_hist.get(str(hour), {})
        hour_quality = ""
        if hour_data.get("n", 0) >= 10:
            hist_acc = hour_data["accuracy"]
            hour_quality = f"(hist acc {hist_acc:.0%})"
            if hour in META_FILTER.get("best_hours_utc", []):
                hour_quality += " Positive"
            elif hour in WORST_HOURS_UTC:
                hour_quality += " Negative"

        lines.append(f"  {now_dt.strftime('%H:%M:%S')} UTC  {hour_quality}")

        for symbol, state in states.items():
            prob_age = f"{time.time() - state.prob_ts:.0f}s" if state.prob_ts else "?"
            position = trader.positions.get(symbol)
            if position and state.price > 0:
                pnl = position.pnl(state.price)
                pos_str = f"{position.direction.upper()} ${pnl:+.2f} (score:{position.meta_score:.2f})"
            else:
                pos_str = "—"

            price_str = f"${state.price:,.4g}" if state.price else "waiting"
            streak_str = f"streak:{state.consecutive_same_dir}" if state.consecutive_same_dir > 1 else ""
            trend_str = TREND.trend_label(state)
            chop_str = CHOP.label(state)
            btc_state = states.get("BTC", state)
            vol_str = VOL.label(state, btc_state)

            lines.append(
                f"  {symbol:<4} | {price_str:<14} | P(up):{state.prob_up:.0%} ({prob_age} ago) | "
                f"Win:{state.secs_left:.0f}s | {trend_str:<12} | {chop_str:<18} | {vol_str:<20} | "
                f"{pos_str} {streak_str}"
            )

        lines.append(f"  {trader.stats()}")

        if tick % 3 == 0:
            live_vs = trader.live_vs_historical_summary()
            if live_vs:
                lines.append(live_vs)

        lines.append(f"{'─' * 70}")
        log.info("\n".join(lines))


async def log_saver(trader: PaperTrader) -> None:
    while True:
        await asyncio.sleep(60)
        if not trader.trade_log:
            continue

        save_json_file(TRADES_PATH, trader.trade_log)
        log.info(f"{len(trader.trade_log)} trades → {TRADES_PATH.name}")

        live_summary = {
            "generated_at": utc_now().isoformat(),
            "trade_count": trader.trade_count,
            "hourly_live": {
                hour: {"n": values["n"], "accuracy": round(values["wins"] / values["n"], 4)}
                for hour, values in trader.hourly_live.items()
                if values["n"] > 0
            },
            "dow_live": {
                day: {"n": values["n"], "accuracy": round(values["wins"] / values["n"], 4)}
                for day, values in trader.dow_live.items()
                if values["n"] > 0
            },
            "conf_live": {
                tier: {"n": values["n"], "accuracy": round(values["wins"] / values["n"], 4)}
                for tier, values in trader.conf_live.items()
                if values["n"] > 0
            },
        }
        save_json_file(LIVE_ACCURACY_PATH, live_summary)


# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    load_meta_filter()

    log.info("=" * 70)
    log.info("  MULTI-ASSET POLYMARKET BOT — 5-MINUTE — PAPER TRADING")
    log.info("=" * 70)
    log.info(f"  Assets       : {', '.join(ASSETS.keys())}")
    log.info(f"  Sizing       : percentage mode={'ON' if CONFIG['USE_PCT_SIZING'] else 'OFF'}")
    log.info(
        f"  Buy >{CONFIG['BUY_THRESHOLD']:.0%} | Sell <{CONFIG['SELL_THRESHOLD']:.0%} | "
        f"No entry in last {CONFIG['MIN_WINDOW_SECONDS_LEFT']}s"
    )
    log.info(
        f"  SL:{CONFIG['STOP_LOSS_PCT']:.1%} | TP:{CONFIG['TAKE_PROFIT_PCT']:.1%} | "
        f"Scratch:{CONFIG['SCRATCH_SECONDS']}s<= {CONFIG['SCRATCH_MAX_PNL_PCT']:.1%} | "
        f"MaxHold:{CONFIG['MAX_HOLD_SECONDS']}s"
    )
    log.info(
        f"  Shorts       : threshold<{CONFIG['SELL_THRESHOLD']:.0%} | "
        f"size×{CONFIG['SHORT_SIZE_MULTIPLIER']:.2f}"
    )
    log.info(
        "  Meta-filter  : "
        + ("ACTIVE — size + gate enabled" if META_FILTER else "DISABLED (no meta_filter.json)")
    )
    log.info(
        f"  Size scaling : META_SIZE_SCALE={CONFIG['META_SIZE_SCALE']} "
        f"({'ON' if CONFIG['META_SIZE_SCALE'] > 0 else 'OFF'})"
    )
    log.info(
        f"  Worst hours  : "
        f"{'BLOCKING ' + str(WORST_HOURS_UTC) if WORST_HOURS_UTC and CONFIG['BLOCK_WORST_HOURS'] else 'not blocking'}"
    )
    log.info("=" * 70 + "\n")

    states = {symbol: AssetState(symbol=symbol) for symbol in ASSETS}
    trader = PaperTrader()
    engine = SignalEngine(trader, states)

    tasks = [
        kraken_feed(states),
        signal_loop(states, engine),
        status_loop(states, trader),
        log_saver(trader),
    ]
    tasks.extend(asset_feed(symbol, states[symbol], trader) for symbol in ASSETS)

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("\nBot stopped.")
