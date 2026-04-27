"""
day_simulator.py — Simulates one trading day minute by minute.

Now uses DayData (pre-loaded in-memory DataFrames) instead of SQLite queries.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
import pandas as pd

from config import StrategyParams
from data_loader import DayData
from strategy import (
    find_atm_strike_from_daydata,
    find_hedge_strike_from_daydata,
    calculate_sl,
    calculate_atr,
    compute_atr_trail_sl,
    compute_hedge_step_sl,
    resample_to_timeframe,
)

logger = logging.getLogger(__name__)


@dataclass
class DayResult:
    date: str
    expiry: str
    status: str
    entry_time: str       = ""

    atm_strike: Optional[int]   = None
    ce_entry:   Optional[float] = None
    pe_entry:   Optional[float] = None

    ce_hedge_strike: Optional[int]   = None
    ce_hedge_entry:  Optional[float] = None
    pe_hedge_strike: Optional[int]   = None
    pe_hedge_entry:  Optional[float] = None

    ce_sl: Optional[float] = None
    pe_sl: Optional[float] = None
    vix_at_entry: Optional[float] = None

    ce_exit:        Optional[float] = None
    ce_exit_reason: str             = ""
    ce_exit_time:   str             = ""
    pe_exit:        Optional[float] = None
    pe_exit_reason: str             = ""
    pe_exit_time:   str             = ""

    ce_hedge_exit:        Optional[float] = None
    ce_hedge_exit_reason: str             = ""
    pe_hedge_exit:        Optional[float] = None
    pe_hedge_exit_reason: str             = ""

    ce_sell_pnl:  float = 0.0
    pe_sell_pnl:  float = 0.0
    ce_hedge_pnl: float = 0.0
    pe_hedge_pnl: float = 0.0
    total_pnl:    float = 0.0
    notes: str = ""

    def compute_pnl(self, lot_size: int):
        if self.ce_entry and self.ce_exit:
            self.ce_sell_pnl = (self.ce_entry - self.ce_exit) * lot_size
        if self.pe_entry and self.pe_exit:
            self.pe_sell_pnl = (self.pe_entry - self.pe_exit) * lot_size
        if self.ce_hedge_entry and self.ce_hedge_exit:
            self.ce_hedge_pnl = (self.ce_hedge_exit - self.ce_hedge_entry) * lot_size
        if self.pe_hedge_entry and self.pe_hedge_exit:
            self.pe_hedge_pnl = (self.pe_hedge_exit - self.pe_hedge_entry) * lot_size
        self.total_pnl = (self.ce_sell_pnl + self.pe_sell_pnl
                          + self.ce_hedge_pnl + self.pe_hedge_pnl)

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items()}


class DaySimulator:
    def __init__(self, params: StrategyParams):
        self.p = params

    def simulate(self, day: DayData) -> DayResult:
        result = DayResult(date=day.date_str, expiry=day.expiry_str, status="ok")

        if not day.is_valid:
            result.status = "no_data"
            result.notes  = "DayData missing VIX / Spot / Options"
            return result

        # Phase 1: ATM selection
        atm_strike, ce_prem, pe_prem, entry_time = find_atm_strike_from_daydata(
            day,
            scan_start=self.p.atm_scan_start,
            scan_end=self.p.atm_scan_end,
            max_premium_diff=self.p.max_premium_diff,
        )

        if atm_strike is None:
            result.status = "no_atm"
            result.notes  = "No ATM strike found within premium diff limit"
            return result

        # Detect fallback marker embedded in entry_time string
        if entry_time and "|FALLBACK" in entry_time:
            entry_time      = entry_time.replace("|FALLBACK", "")
            result.notes    = "ATM_FALLBACK: spot-based strike used (premium diff not met)"

        result.atm_strike = atm_strike
        # Apply entry slippage:
        # SELL legs (CE/PE): we get worse fill = lower price (sell at lower price)
        # BUY legs (hedges): we pay more = higher price (buy at higher price)
        slip = self.p.slippage_pct
        result.ce_entry   = round(ce_prem * (1 - slip), 2) if slip > 0 else ce_prem
        result.pe_entry   = round(pe_prem * (1 - slip), 2) if slip > 0 else pe_prem
        result.entry_time = entry_time

        # Phase 1b: Hedge selection
        ce_hedge_strike, ce_hedge_entry = find_hedge_strike_from_daydata(
            day, entry_time, atm_strike, ce_prem, self.p.hedge_pct, "CE"
        )
        pe_hedge_strike, pe_hedge_entry = find_hedge_strike_from_daydata(
            day, entry_time, atm_strike, pe_prem, self.p.hedge_pct, "PE"
        )

        result.ce_hedge_strike = ce_hedge_strike
        # Hedge buys: we pay more (higher price = worse fill for buyer)
        result.ce_hedge_entry  = round(ce_hedge_entry * (1 + slip), 2) if (ce_hedge_entry and slip > 0) else ce_hedge_entry
        result.pe_hedge_strike = pe_hedge_strike
        result.pe_hedge_entry  = round(pe_hedge_entry * (1 + slip), 2) if (pe_hedge_entry and slip > 0) else pe_hedge_entry

        # Phase 1c: VIX-based SL
        vix_at_entry = _get_value_at_time(day.vix_1min, entry_time)
        if vix_at_entry is None:
            vix_at_entry = day.vix_prev_close
        result.vix_at_entry = vix_at_entry

        result.ce_sl = calculate_sl(
            ce_prem, day.vix_prev_close, vix_at_entry,
            vix_intraday_threshold=self.p.vix_intraday_threshold,
            sl_buffer=self.p.sl_buffer,
        )
        result.pe_sl = calculate_sl(
            pe_prem, day.vix_prev_close, vix_at_entry,
            vix_intraday_threshold=self.p.vix_intraday_threshold,
            sl_buffer=self.p.sl_buffer,
        )

        # Fetch per-strike series
        ce_series  = day.options_1min.get((atm_strike,      "CE"), pd.DataFrame())
        pe_series  = day.options_1min.get((atm_strike,      "PE"), pd.DataFrame())
        ceh_series = day.options_1min.get((ce_hedge_strike, "CE"), pd.DataFrame()) if ce_hedge_strike else pd.DataFrame()
        peh_series = day.options_1min.get((pe_hedge_strike, "PE"), pd.DataFrame()) if pe_hedge_strike else pd.DataFrame()

        if ce_series.empty or pe_series.empty:
            result.status = "no_data"
            result.notes  = f"CE or PE series missing for strike {atm_strike}"
            return result

        result = self._simulate_loop(result, entry_time, ce_series, pe_series,
                                     ceh_series, peh_series)
        result.compute_pnl(self.p.lot_size)
        return result

    def _simulate_loop(self, result, entry_time, ce_series, pe_series,
                       ceh_series, peh_series) -> DayResult:
        """
        Fully vectorized 1-second simulation.
        ATR SL pre-computed ONCE per leg — eliminates 11,000 DataFrame
        constructions per day that caused the slowdown.
        """
        import numpy as np
        eod_ts = pd.Timestamp(f"{result.date} {self.p.eod_exit_time}")
        slip   = self.p.slippage_pct

        entry_ts = pd.Timestamp(f"{result.date} {entry_time}")
        ce  = ce_series[(ce_series.index >= entry_ts) & (ce_series.index <= eod_ts)]
        pe  = pe_series[(pe_series.index >= entry_ts) & (pe_series.index <= eod_ts)]
        ceh = ceh_series[(ceh_series.index >= entry_ts) & (ceh_series.index <= eod_ts)] \
              if not ceh_series.empty else pd.DataFrame()
        peh = peh_series[(peh_series.index >= entry_ts) & (peh_series.index <= eod_ts)] \
              if not peh_series.empty else pd.DataFrame()

        if ce.empty or pe.empty:
            return result

        ce_sl = result.ce_sl
        pe_sl = result.pe_sl

        # Pre-compute ATR SL series for both legs (1 resample + 1 ewm each)
        ce_atr_sl = self._precompute_atr_sl(ce_series, ce_sl)
        pe_atr_sl = self._precompute_atr_sl(pe_series, pe_sl)

        # Fixed SL: first breach (vectorized numpy)
        ce_hit = _first_breach_idx(ce, ce_sl)
        pe_hit = _first_breach_idx(pe, pe_sl)

        ce_first = ce_hit is not None and (pe_hit is None or ce_hit <= pe_hit)
        pe_first = pe_hit is not None and (ce_hit is None or pe_hit < ce_hit)

        if ce_hit is None and pe_hit is None:
            return _eod_exit_all(result, ce, pe, ceh, peh, eod_ts, slippage_pct=slip)

        if ce_first:
            idx_ts = ce.index[ce_hit]
            result.ce_exit        = _apply_slippage(ce_sl, slip)
            result.ce_exit_reason = "FIXED_SL"
            result.ce_exit_time   = idx_ts.strftime("%H:%M:%S")

            ceh_after = ceh[ceh.index >= idx_ts] if not ceh.empty else pd.DataFrame()
            result = _trail_hedge(result, ceh_after, result.ce_hedge_entry or 0.0,
                                  self.p.hedge_trail_step, "ce")
            result = self._atr_trail_leg_fast(result, pe, pe_sl, eod_ts, "pe",
                                              idx_ts, pe_atr_sl, slip)
            if not peh.empty and result.pe_hedge_exit is None:
                peh_eod = peh[peh.index <= eod_ts]
                if not peh_eod.empty:
                    result.pe_hedge_exit        = float(peh_eod["close"].iloc[-1])
                    result.pe_hedge_exit_reason = "EOD"

        elif pe_first:
            idx_ts = pe.index[pe_hit]
            result.pe_exit        = _apply_slippage(pe_sl, slip)
            result.pe_exit_reason = "FIXED_SL"
            result.pe_exit_time   = idx_ts.strftime("%H:%M:%S")

            peh_after = peh[peh.index >= idx_ts] if not peh.empty else pd.DataFrame()
            result = _trail_hedge(result, peh_after, result.pe_hedge_entry or 0.0,
                                  self.p.hedge_trail_step, "pe")
            result = self._atr_trail_leg_fast(result, ce, ce_sl, eod_ts, "ce",
                                              idx_ts, ce_atr_sl, slip)
            if not ceh.empty and result.ce_hedge_exit is None:
                ceh_eod = ceh[ceh.index <= eod_ts]
                if not ceh_eod.empty:
                    result.ce_hedge_exit        = float(ceh_eod["close"].iloc[-1])
                    result.ce_hedge_exit_reason = "EOD"

        return result

    def compute_day_entry(self, day) -> Optional[dict]:
        """
        Pre-compute ATM + hedge entry for a day.
        Result can be cached and reused across all grid combos
        that share the same ATM/hedge params.
        Returns None if no ATM found.
        """
        from strategy import find_atm_strike_from_daydata, find_hedge_strike_from_daydata

        atm_strike, ce_prem, pe_prem, entry_time = find_atm_strike_from_daydata(
            day,
            self.p.atm_scan_start,
            self.p.atm_scan_end,
            self.p.max_premium_diff,
        )
        if atm_strike is None:
            return None

        # Detect fallback
        is_fallback = False
        if entry_time and "|FALLBACK" in str(entry_time):
            entry_time = entry_time.replace("|FALLBACK", "")
            is_fallback = True

        # Apply entry slippage
        slip = self.p.slippage_pct
        ce_entry = round(ce_prem * (1 - slip), 2) if slip > 0 else ce_prem
        pe_entry = round(pe_prem * (1 - slip), 2) if slip > 0 else pe_prem

        # Hedge
        ce_hs, ce_he = find_hedge_strike_from_daydata(
            day, entry_time, atm_strike, ce_prem, self.p.hedge_pct, "CE"
        )
        pe_hs, pe_he = find_hedge_strike_from_daydata(
            day, entry_time, atm_strike, pe_prem, self.p.hedge_pct, "PE"
        )
        ce_hedge_entry = round(ce_he * (1 + slip), 2) if (ce_he and slip > 0) else ce_he
        pe_hedge_entry = round(pe_he * (1 + slip), 2) if (pe_he and slip > 0) else pe_he

        return {
            "atm_strike":     atm_strike,
            "ce_entry":       ce_entry,
            "pe_entry":       pe_entry,
            "entry_time":     entry_time,
            "is_fallback":    is_fallback,
            "ce_hedge_strike":ce_hs,
            "ce_hedge_entry": ce_hedge_entry,
            "pe_hedge_strike":pe_hs,
            "pe_hedge_entry": pe_hedge_entry,
        }

    def simulate_with_entry(self, day, entry: dict) -> "DayResult":
        """
        Simulate using pre-computed ATM+hedge entry — skips find_atm/hedge (~80% faster).
        """
        result = DayResult(date=day.date_str, expiry=day.expiry_str, status="ok")
        result.atm_strike      = entry["atm_strike"]
        result.ce_entry        = entry["ce_entry"]
        result.pe_entry        = entry["pe_entry"]
        result.entry_time      = entry["entry_time"]
        result.ce_hedge_strike = entry["ce_hedge_strike"]
        result.ce_hedge_entry  = entry["ce_hedge_entry"]
        result.pe_hedge_strike = entry["pe_hedge_strike"]
        result.pe_hedge_entry  = entry["pe_hedge_entry"]
        if entry.get("is_fallback"):
            result.notes = "ATM_FALLBACK: spot-based strike used (premium diff not met)"

        entry_time = entry["entry_time"]

        # VIX SL — same as simulate(), respects per-combo SL params
        vix_at_entry = _get_value_at_time(day.vix_1min, entry_time)
        if vix_at_entry is None:
            vix_at_entry = day.vix_prev_close
        result.vix_at_entry = vix_at_entry

        result.ce_sl = calculate_sl(
            entry["ce_entry"] or 0, day.vix_prev_close, vix_at_entry,
            vix_intraday_threshold=self.p.vix_intraday_threshold,
            sl_buffer=self.p.sl_buffer,
        )
        result.pe_sl = calculate_sl(
            entry["pe_entry"] or 0, day.vix_prev_close, vix_at_entry,
            vix_intraday_threshold=self.p.vix_intraday_threshold,
            sl_buffer=self.p.sl_buffer,
        )

        atm = entry["atm_strike"]
        ce_series  = day.options_1min.get((atm, "CE"), pd.DataFrame())
        pe_series  = day.options_1min.get((atm, "PE"), pd.DataFrame())
        ceh_series = day.options_1min.get((entry["ce_hedge_strike"], "CE"), pd.DataFrame()) \
                     if entry.get("ce_hedge_strike") else pd.DataFrame()
        peh_series = day.options_1min.get((entry["pe_hedge_strike"], "PE"), pd.DataFrame()) \
                     if entry.get("pe_hedge_strike") else pd.DataFrame()

        if ce_series.empty or pe_series.empty:
            result.status = "no_data"
            result.notes  = f"CE or PE series missing for strike {atm}"
            return result

        result = self._simulate_loop(result, entry_time,
                                      ce_series, pe_series,
                                      ceh_series, peh_series)
        result.compute_pnl(self.p.lot_size)
        return result

    def _precompute_atr_sl(self, series: pd.DataFrame,
                            initial_sl: float) -> pd.Series:
        """
        Compute ATR trailing SL for every ATR candle in ONE pass.
        Replaces 11,000 per-second DataFrame builds with:
          - 1 resample
          - 1 vectorized ewm ATR
          - 1 cumulative min walk
        """
        if series.empty or initial_sl is None:
            return pd.Series(dtype=float)

        candles = resample_to_timeframe(series, self.p.atr_timeframe)
        if len(candles) < self.p.atr_period + 1:
            return pd.Series(dtype=float)

        atr = calculate_atr(candles, self.p.atr_period)
        raw_sl = candles["close"] + self.p.atr_multiplier * atr

        # Walk forward: only tighten SL, start from initial_sl
        sl_vals = []
        current = initial_sl
        for i, sl_val in enumerate(raw_sl):
            if i >= self.p.atr_period and not pd.isna(sl_val) and sl_val < current:
                current = sl_val
            sl_vals.append(current)

        return pd.Series(sl_vals, index=candles.index, dtype=float)

    def _atr_trail_leg_fast(self, result, series, initial_sl, eod_ts,
                             leg: str, trail_start_ts, atr_sl_series,
                             slippage_pct=0.0) -> DayResult:
        """
        Fast ATR trailing using pre-computed SL lookup table.
        Per candle: O(1) SL lookup + vectorized numpy breach check.
        No DataFrame construction inside loop.
        """
        if series.empty or atr_sl_series.empty:
            _set_eod(result, series, leg, slippage_pct=slippage_pct)
            return result

        active = atr_sl_series[atr_sl_series.index > trail_start_ts]
        ticks  = series[series.index > trail_start_ts]
        if ticks.empty:
            _set_eod(result, series, leg, slippage_pct=slippage_pct)
            return result

        prev_ts = trail_start_ts
        for candle_ts, sl_price in active.items():
            if candle_ts >= eod_ts:
                _set_eod(result, series, leg, slippage_pct=slippage_pct)
                return result

            tick_win = ticks[(ticks.index > prev_ts) & (ticks.index <= candle_ts)]
            prev_ts  = candle_ts
            if tick_win.empty:
                continue

            breach = _first_breach_idx(tick_win, sl_price)
            if breach is not None:
                _set_sl_exit(result, sl_price, tick_win.index[breach],
                             "ATR_TRAIL_SL", leg, slippage_pct=slippage_pct)
                return result

        _set_eod(result, series, leg, slippage_pct=slippage_pct)
        return result

# ── Vectorized helpers ────────────────────────────────────────────────────────

def _apply_slippage(price: float, slippage_pct: float, is_short: bool = True) -> float:
    """
    Apply slippage to an exit price.
    For SHORT positions (selling options): slippage increases exit price (costs more to buy back)
    slippage_pct = 0.001 means 0.1%
    """
    if slippage_pct <= 0:
        return price
    if is_short:
        return round(price * (1 + slippage_pct), 2)
    else:
        return round(price * (1 - slippage_pct), 2)


def _first_breach_idx(series: pd.DataFrame, sl: float) -> Optional[int]:
    """
    Find index of FIRST second where high >= sl.
    Pure numpy — single array operation regardless of series length.
    Returns None if never breached.
    """
    if series.empty or sl is None or "high" not in series.columns:
        return None
    import numpy as np
    highs = series["high"].values
    mask  = highs >= sl
    if not mask.any():
        return None
    return int(np.argmax(mask))   # argmax returns first True index


def _trail_hedge(result, hedge_series: pd.DataFrame, entry_price: float,
                 step: float, leg: str) -> DayResult:
    """
    Step-wise hedge trailing — fully vectorized with numpy.

    Logic:
    1. Compute cumulative max of close prices (numpy cummax)
    2. Compute step SL for each point: SL = entry + (floor((max-entry)/step) - 1) * step
    3. Find first index where low <= computed SL (numpy argmax on boolean mask)
    No Python loops — O(n) numpy operations only.
    """
    import numpy as np
    if hedge_series.empty:
        return result

    closes = hedge_series["close"].values
    lows   = hedge_series["low"].values if "low" in hedge_series.columns else closes

    # Step 1: Running max (vectorized cummax)
    running_max = np.maximum.accumulate(np.maximum(closes, entry_price))

    # Step 2: Compute step SL at each point vectorized
    # steps_completed = floor((running_max - entry) / step)
    # SL = entry + (steps_completed - 1) * step  when steps_completed >= 1
    gain         = running_max - entry_price
    steps        = np.floor(gain / step).astype(int)
    sl_prices    = np.where(steps >= 1,
                            entry_price + (steps - 1) * step,
                            np.nan)

    # Step 3: Find first index where low <= sl_price (SL hit)
    # Only consider indices where sl is active (not nan)
    sl_active = ~np.isnan(sl_prices)
    sl_hit    = sl_active & (lows <= sl_prices)

    if sl_hit.any():
        idx = int(np.argmax(sl_hit))
        sl  = float(sl_prices[idx])
        ts  = hedge_series.index[idx]
        if leg == "ce":
            result.ce_hedge_exit        = sl
            result.ce_hedge_exit_reason = "STEP_TRAIL_SL"
        else:
            result.pe_hedge_exit        = sl
            result.pe_hedge_exit_reason = "STEP_TRAIL_SL"
        return result

    # No step SL hit → EOD
    last_close = float(closes[-1]) if len(closes) else None
    if leg == "ce":
        result.ce_hedge_exit        = last_close
        result.ce_hedge_exit_reason = "EOD"
    else:
        result.pe_hedge_exit        = last_close
        result.pe_hedge_exit_reason = "EOD"
    return result


def _eod_exit_all(result, ce, pe, ceh, peh, eod_ts, slippage_pct=0.0) -> DayResult:
    """Square off all legs at EOD with slippage applied."""
    for series, attr_exit, attr_reason, attr_time, is_short in [
        (ce,  "ce_exit",       "ce_exit_reason",       "ce_exit_time", True),
        (pe,  "pe_exit",       "pe_exit_reason",       "pe_exit_time", True),
        (ceh, "ce_hedge_exit", "ce_hedge_exit_reason", None,           False),
        (peh, "pe_hedge_exit", "pe_hedge_exit_reason", None,           False),
    ]:
        if series.empty:
            continue
        last = series[series.index <= eod_ts]
        if last.empty:
            last = series
        price = _apply_slippage(float(last["close"].iloc[-1]), slippage_pct, is_short=is_short)
        setattr(result, attr_exit,  price)
        setattr(result, attr_reason, "EOD")
        if attr_time:
            setattr(result, attr_time, last.index[-1].strftime("%H:%M:%S"))
    return result


def _set_sl_exit(result, sl_price, ts, reason, leg, slippage_pct=0.0):
    time_str   = ts.strftime("%H:%M:%S")
    exit_price = _apply_slippage(sl_price, slippage_pct, is_short=True)
    if leg == "ce":
        result.ce_exit = exit_price; result.ce_exit_reason = reason
        result.ce_exit_time = time_str
    else:
        result.pe_exit = exit_price; result.pe_exit_reason = reason
        result.pe_exit_time = time_str


def _set_eod(result, series, leg, slippage_pct=0.0):
    if series.empty: return
    price    = _apply_slippage(float(series["close"].iloc[-1]), slippage_pct, is_short=True)
    time_str = series.index[-1].strftime("%H:%M:%S")
    if leg == "ce":
        result.ce_exit = price; result.ce_exit_reason = "EOD"
        result.ce_exit_time = time_str
    else:
        result.pe_exit = price; result.pe_exit_reason = "EOD"
        result.pe_exit_time = time_str


def _get_value_at_time(series, time_str, col="close"):
    """Get value at HH:MM using fast Timestamp comparison."""
    if series.empty: return None
    date_str = series.index[0].strftime("%Y-%m-%d")
    ts0 = pd.Timestamp(f"{date_str} {time_str}:00")
    ts1 = pd.Timestamp(f"{date_str} {time_str}:59")
    m   = series[(series.index >= ts0) & (series.index <= ts1)]
    if m.empty:
        before = series[series.index <= ts1]
        return float(before[col].iloc[-1]) if not before.empty else None
    return float(m[col].iloc[-1])
