"""
strategy.py — Pure, stateless strategy functions.

All functions here are deterministic given the same inputs.
No I/O, no state, no side-effects — easy to unit test.
"""

import math
import numpy as np
import pandas as pd
from typing import Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# ATM SELECTION
# ─────────────────────────────────────────────────────────────────────────────

def find_atm_strike(
    options_df: pd.DataFrame,
    scan_start: str,    # "09:16" HH:MM
    scan_end: str,      # "09:21" HH:MM
    max_premium_diff: float,
) -> Tuple[Optional[int], Optional[float], Optional[float], Optional[str]]:
    """
    Scan options data between scan_start and scan_end to find the ATM strike.

    Logic:
        For each minute in the scan window, look at all available strikes.
        For each strike, compute |CE_close - PE_close|.
        Pick the strike with minimum |CE - PE| that is also ≤ max_premium_diff.

    Returns:
        (strike, ce_premium, pe_premium, entry_time_str) or (None, None, None, None)
    """
    if options_df.empty:
        return None, None, None, None

    df = options_df.copy()
    df["time"] = df["datetime"].dt.strftime("%H:%M")

    # Filter to scan window
    window = df[(df["time"] >= scan_start) & (df["time"] <= scan_end)]
    if window.empty:
        return None, None, None, None

    best_strike   = None
    best_ce_prem  = None
    best_pe_prem  = None
    best_diff     = float("inf")
    best_time     = None

    # Iterate each minute in the window
    for ts, grp in window.groupby("datetime"):
        ce_grp = grp[grp["opt_type"] == "CE"].set_index("strike")["close"]
        pe_grp = grp[grp["opt_type"] == "PE"].set_index("strike")["close"]

        # Only strikes that have BOTH CE and PE prices
        common_strikes = ce_grp.index.intersection(pe_grp.index)
        if common_strikes.empty:
            continue

        for strike in common_strikes:
            ce_p = ce_grp[strike]
            pe_p = pe_grp[strike]
            if pd.isna(ce_p) or pd.isna(pe_p) or ce_p <= 0 or pe_p <= 0:
                continue
            diff = abs(ce_p - pe_p)
            if diff < best_diff:
                best_diff    = diff
                best_strike  = strike
                best_ce_prem = ce_p
                best_pe_prem = pe_p
                best_time    = ts.strftime("%H:%M")

    if best_diff > max_premium_diff:
        return None, None, None, None   # No strike within acceptable diff

    return best_strike, best_ce_prem, best_pe_prem, best_time


def find_hedge_strike(
    options_df: pd.DataFrame,
    entry_time: str,         # "HH:MM" — use the same candle as ATM entry
    atm_strike: int,
    sell_premium: float,
    hedge_pct: float,        # e.g. 0.05 = 5%
    opt_type: str,           # "CE" or "PE"
) -> Tuple[Optional[int], Optional[float]]:
    """
    Find the OTM strike whose premium is closest to (sell_premium * hedge_pct).

    For CE hedge: strike > atm_strike (OTM call)
    For PE hedge: strike < atm_strike (OTM put)

    Returns: (hedge_strike, hedge_premium) or (None, None)
    """
    target = sell_premium * hedge_pct

    df = options_df.copy()
    df["time"] = df["datetime"].dt.strftime("%H:%M")

    at_entry = df[(df["time"] == entry_time) & (df["opt_type"] == opt_type)]

    if opt_type == "CE":
        candidates = at_entry[at_entry["strike"] > atm_strike]
    else:
        candidates = at_entry[at_entry["strike"] < atm_strike]

    candidates = candidates[candidates["close"] > 0].copy()
    if candidates.empty:
        return None, None

    candidates["diff"] = abs(candidates["close"] - target)
    best = candidates.loc[candidates["diff"].idxmin()]
    return int(best["strike"]), float(best["close"])


# ─────────────────────────────────────────────────────────────────────────────
# STOP LOSS CALCULATION
# ─────────────────────────────────────────────────────────────────────────────

def round_up_to_5(value: float) -> float:
    """Round value UP to the nearest multiple of 5."""
    return math.ceil(value / 5.0) * 5.0


def calculate_sl(
    sell_premium: float,
    vix_prev_close: float,
    vix_current: float,
    vix_intraday_threshold: float = 3.0,
    # VIX regime boundaries (user-configurable)
    vix_low: float      = 12.0,   # below this → regime 1
    vix_mid_low: float  = 16.0,   # vix_low–vix_mid_low → regime 2
    vix_mid_high: float = 20.0,   # vix_mid_low–vix_mid_high → regime 3, above → regime 4
    # SL percentages per regime
    sl_pct_r1: float          = 0.40,  # VIX < vix_low
    sl_pct_r2_calm: float     = 0.40,  # vix_low ≤ VIX < vix_mid_low, calm
    sl_pct_r2_volatile: float = 0.25,  # vix_low ≤ VIX < vix_mid_low, volatile
    sl_pct_r3: float          = 0.25,  # vix_mid_low ≤ VIX < vix_mid_high
    sl_pct_r4: float          = 0.15,  # VIX ≥ vix_mid_high
    sl_buffer: float          = 5.0,
) -> float:
    """
    Calculate stop loss for a sold option leg based on VIX regime.

    VIX regimes (boundaries fully configurable):
        VIX < vix_low                              → sl_pct_r1
        vix_low ≤ VIX < vix_mid_low, calm          → sl_pct_r2_calm
        vix_low ≤ VIX < vix_mid_low, volatile       → sl_pct_r2_volatile
        vix_mid_low ≤ VIX < vix_mid_high            → sl_pct_r3
        VIX ≥ vix_mid_high                          → sl_pct_r4

    Intraday VIX move = (vix_current - vix_prev_close) / vix_prev_close * 100
    SL is rounded UP to nearest 5.
    """
    if vix_prev_close and vix_prev_close > 0:
        vix_intraday_move_pct = (vix_current - vix_prev_close) / vix_prev_close * 100.0
    else:
        vix_intraday_move_pct = 0.0

    # Determine SL % based on configurable boundaries
    if vix_current < vix_low:
        sl_pct = sl_pct_r1
    elif vix_current < vix_mid_low:
        if abs(vix_intraday_move_pct) > vix_intraday_threshold:
            sl_pct = sl_pct_r2_volatile
        else:
            sl_pct = sl_pct_r2_calm
    elif vix_current < vix_mid_high:
        sl_pct = sl_pct_r3
    else:
        sl_pct = sl_pct_r4

    raw_sl = sell_premium + (sell_premium * sl_pct) + sl_buffer
    return round_up_to_5(raw_sl)


# ─────────────────────────────────────────────────────────────────────────────
# ATR CALCULATION
# ─────────────────────────────────────────────────────────────────────────────

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Compute ATR on a DataFrame with columns [open, high, low, close].
    Returns a Series of ATR values aligned to df's index.
    """
    high  = df["high"]
    low   = df["low"]
    close = df["close"]

    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)

    atr = tr.ewm(span=period, adjust=False).mean()
    return atr


def resample_to_timeframe(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Resample 1-min option OHLC to a higher timeframe.
    timeframe: "1min", "5min", "15min"
    df must have datetime index.
    """
    if timeframe == "1min":
        return df

    freq_map = {"5min": "5min", "15min": "15min", "30min": "30min"}
    freq = freq_map.get(timeframe, "5T")

    agg = {"open": "first", "high": "max", "low": "min", "close": "last"}
    if "volume" in df.columns:
        agg["volume"] = "sum"
    resampled = df.resample(freq, label="right", closed="right").agg(agg).dropna(subset=["close"])
    return resampled


def compute_atr_trail_sl(
    series: pd.DataFrame,   # Resampled candles up to current time
    period: int,
    multiplier: float,
    is_short: bool = True,  # True = we are short the option
) -> Optional[float]:
    """
    Compute the current ATR-based trailing SL for a position.

    For SHORT positions: SL = last_close + multiplier * ATR
    (If price rises above this, we cover.)

    Returns None if not enough data.
    """
    if len(series) < period + 1:
        return None

    atr_series = calculate_atr(series, period)
    last_atr   = atr_series.iloc[-1]
    last_close = series["close"].iloc[-1]

    if pd.isna(last_atr) or last_atr == 0:
        return None

    if is_short:
        return last_close + multiplier * last_atr
    else:
        return last_close - multiplier * last_atr


# ─────────────────────────────────────────────────────────────────────────────
# HEDGE STEP TRAILING
# ─────────────────────────────────────────────────────────────────────────────

def compute_hedge_step_sl(
    max_price_seen: float,
    entry_price: float,
    step: float = 3.0,
) -> Optional[float]:
    """
    Compute the current stop loss for the hedge leg using step trailing.

    Logic:
        Milestones: entry, entry+step, entry+2*step, entry+3*step, ...
        When price reaches milestone[n], SL = milestone[n-1]
        SL is based on the HIGHEST price seen (not current price).

    Examples (entry=5, step=3):
        max_seen < 8  → SL = None  (no SL yet)
        max_seen = 8  → SL = 5
        max_seen = 11 → SL = 8
        max_seen = 14 → SL = 11

    Returns None if hedge hasn't risen enough to trigger first SL.
    """
    if max_price_seen < entry_price + step:
        return None

    steps_completed = int((max_price_seen - entry_price) / step)
    sl = entry_price + (steps_completed - 1) * step
    return max(sl, entry_price)  # Never below breakeven


# ─────────────────────────────────────────────────────────────────────────────
# NIFTY EXPIRY UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

_EXPIRY_CHANGE_DATE_STR = "2025-09-01"

def get_nearest_expiry(trading_date: str, expiry_weekday: int = None) -> str:
    """
    Get nearest NIFTY weekly expiry on or after trading_date.
    Before Sep 1 2025: Thursday (3)
    From  Sep 1 2025 : Tuesday  (1)
    """
    from datetime import date, timedelta
    d = date.fromisoformat(trading_date)
    if expiry_weekday is None:
        expiry_weekday = 1 if trading_date >= _EXPIRY_CHANGE_DATE_STR else 3
    days_ahead = expiry_weekday - d.weekday()
    if days_ahead < 0:
        days_ahead += 7
    expiry = d + timedelta(days=days_ahead)
    return expiry.strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────────────────────
# DAYDATA-AWARE WRAPPERS
# (Uses pre-loaded DayData instead of raw DataFrame)
# ─────────────────────────────────────────────────────────────────────────────

def find_atm_strike_from_daydata(
    day,                        # DayData
    scan_start: str,
    scan_end: str,
    max_premium_diff: float,
) -> Tuple[Optional[int], Optional[float], Optional[float], Optional[str]]:
    """
    Find ATM strike from DayData.options_1min dict.

    Strategy:
    1. Use NIFTY spot price at scan_start to narrow candidates to ±500pts around spot
    2. For each candidate strike, get last available CE and PE close in the scan window
       (does NOT require exact timestamp match — handles sparse tick data)
    3. Pick strike with minimum |CE - PE| within max_premium_diff
    """
    best_strike  = None
    best_ce_prem = None
    best_pe_prem = None
    best_diff    = float("inf")
    best_time    = None

    # Step 1: Get spot price to narrow ATM candidates
    spot_price = _get_spot_at_time(day, scan_start, scan_end)

    # Collect all strikes that have both CE and PE
    ce_strikes = {s for (s, t) in day.options_1min if t == "CE"}
    pe_strikes = {s for (s, t) in day.options_1min if t == "PE"}
    common     = sorted(ce_strikes & pe_strikes)

    if not common:
        return None, None, None, None

    # Step 2: Narrow to ±500pts around spot if spot is available
    if spot_price:
        candidates = [s for s in common if abs(s - spot_price) <= 500]
        if not candidates:
            candidates = common  # fallback: use all strikes
    else:
        candidates = common

    for strike in candidates:
        ce_df = day.options_1min[(strike, "CE")]
        pe_df = day.options_1min[(strike, "PE")]

        if ce_df.empty or pe_df.empty:
            continue

        # Filter to scan window
        ce_times = ce_df.index.strftime("%H:%M")
        pe_times = pe_df.index.strftime("%H:%M")
        ce_win = ce_df[(ce_times >= scan_start) & (ce_times <= scan_end)]
        pe_win = pe_df[(pe_times >= scan_start) & (pe_times <= scan_end)]

        # Extend window backward if no data found (up to 09:15)
        if ce_win.empty:
            ce_win = ce_df[ce_df.index.strftime("%H:%M") <= scan_end]
        if pe_win.empty:
            pe_win = pe_df[pe_df.index.strftime("%H:%M") <= scan_end]

        if ce_win.empty or pe_win.empty:
            continue

        # Use last available close in window for each leg independently
        ce_p = float(ce_win["close"].iloc[-1])
        pe_p = float(pe_win["close"].iloc[-1])
        ts   = max(ce_win.index[-1], pe_win.index[-1])

        if pd.isna(ce_p) or pd.isna(pe_p) or ce_p <= 0 or pe_p <= 0:
            continue

        diff = abs(ce_p - pe_p)
        if diff < best_diff:
            best_diff    = diff
            best_strike  = strike
            best_ce_prem = ce_p
            best_pe_prem = pe_p
            best_time    = ts.strftime("%H:%M")

    if best_diff > max_premium_diff or best_strike is None:
        return None, None, None, None

    return best_strike, best_ce_prem, best_pe_prem, best_time


def _get_spot_at_time(day, scan_start: str, scan_end: str) -> Optional[float]:
    """Get NIFTY spot price during the scan window from spot_1min data."""
    if day.spot_1min is None or day.spot_1min.empty:
        return None
    times = day.spot_1min.index.strftime("%H:%M")
    win   = day.spot_1min[(times >= scan_start) & (times <= scan_end)]
    if win.empty:
        win = day.spot_1min[times <= scan_end]
    if win.empty:
        return None
    return float(win["close"].iloc[-1])


def find_hedge_strike_from_daydata(
    day,                    # DayData
    entry_time: str,
    atm_strike: int,
    sell_premium: float,
    hedge_pct: float,
    opt_type: str,          # "CE" or "PE"
) -> Tuple[Optional[int], Optional[float]]:
    """
    Find hedge strike from DayData.options_1min dict.
    """
    target = sell_premium * hedge_pct

    best_strike = None
    best_prem   = None
    best_diff   = float("inf")

    for (strike, otype), df in day.options_1min.items():
        if otype != opt_type:
            continue
        if opt_type == "CE" and strike <= atm_strike:
            continue
        if opt_type == "PE" and strike >= atm_strike:
            continue
        if df.empty:
            continue

        # Get close at entry time
        mask = df.index.strftime("%H:%M") == entry_time
        rows = df[mask]
        if rows.empty:
            # Fallback: closest time at or before entry
            before = df[df.index.strftime("%H:%M") <= entry_time]
            if before.empty:
                continue
            price = float(before["close"].iloc[-1])
        else:
            price = float(rows["close"].iloc[0])

        if price <= 0:
            continue

        diff = abs(price - target)
        if diff < best_diff:
            best_diff   = diff
            best_strike = strike
            best_prem   = price

    return best_strike, best_prem
