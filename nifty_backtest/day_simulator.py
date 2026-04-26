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
        Vectorized 1-second simulation using numpy array operations.

        Key speedup: instead of a Python loop over every second, we use
        numpy to find the FIRST second a price breaches SL — this is a
        single array operation (np.argmax) regardless of how many seconds
        are in the day.

        ATR trailing: resampled to ATR timeframe, then SL updated every
        N seconds (once per ATR candle), not every single second.
        """
        import numpy as np

        eod_ts   = pd.Timestamp(f"{result.date} {self.p.eod_exit_time}")
        date_str = result.date

        # Slice to [entry_time, EOD] window
        entry_ts = pd.Timestamp(f"{date_str} {entry_time}")
        ce  = ce_series[(ce_series.index >= entry_ts) & (ce_series.index <= eod_ts)]
        pe  = pe_series[(pe_series.index >= entry_ts) & (pe_series.index <= eod_ts)]
        ceh = ceh_series[(ceh_series.index >= entry_ts) & (ceh_series.index <= eod_ts)]               if not ceh_series.empty else pd.DataFrame()
        peh = peh_series[(peh_series.index >= entry_ts) & (peh_series.index <= eod_ts)]               if not peh_series.empty else pd.DataFrame()

        if ce.empty or pe.empty:
            return result

        ce_sl = result.ce_sl
        pe_sl = result.pe_sl

        # ── Phase 1: Fixed SL — find first breach on either leg (vectorized) ──
        ce_hit_idx = _first_breach_idx(ce, ce_sl)
        pe_hit_idx = _first_breach_idx(pe, pe_sl)

        # Determine which leg hits first (or neither)
        ce_hit_first = (ce_hit_idx is not None and
                        (pe_hit_idx is None or ce_hit_idx <= pe_hit_idx))
        pe_hit_first = (pe_hit_idx is not None and
                        (ce_hit_idx is None or pe_hit_idx < ce_hit_idx))

        # Both miss SL → EOD exit
        if ce_hit_idx is None and pe_hit_idx is None:
            result = _eod_exit_all(result, ce, pe, ceh, peh, eod_ts, slippage_pct=self.p.slippage_pct)
            return result

        # ── Phase 2: First leg exits at fixed SL ──────────────────────────────
        if ce_hit_first:
            idx_ts = ce.index[ce_hit_idx]
            result.ce_exit        = ce_sl
            result.ce_exit_reason = "FIXED_SL"
            result.ce_exit_time   = idx_ts.strftime("%H:%M:%S")

            # CE hedge → step trailing from this point forward
            ceh_after = ceh[ceh.index >= idx_ts] if not ceh.empty else pd.DataFrame()
            result = _trail_hedge(result, ceh_after, result.ce_hedge_entry or 0.0,
                                  self.p.hedge_trail_step, "ce")

            # PE surviving leg → ATR trailing
            # Pass FULL pe series for ATR history, but only check breaches after idx_ts
            result = self._atr_trail_leg(result, pe, pe_sl, eod_ts, "pe",
                                          trail_start_ts=idx_ts)

            # PE hedge → close at EOD price regardless of how PE sell exited
            if not peh.empty and result.pe_hedge_exit is None:
                peh_at_eod = peh[peh.index <= eod_ts]
                if not peh_at_eod.empty:
                    result.pe_hedge_exit        = float(peh_at_eod["close"].iloc[-1])
                    result.pe_hedge_exit_reason = "EOD"

        elif pe_hit_first:
            idx_ts = pe.index[pe_hit_idx]
            result.pe_exit        = pe_sl
            result.pe_exit_reason = "FIXED_SL"
            result.pe_exit_time   = idx_ts.strftime("%H:%M:%S")

            # PE hedge → step trailing
            peh_after = peh[peh.index >= idx_ts] if not peh.empty else pd.DataFrame()
            result = _trail_hedge(result, peh_after, result.pe_hedge_entry or 0.0,
                                  self.p.hedge_trail_step, "pe")

            # CE surviving leg → ATR trailing
            # Pass FULL ce series for ATR history, but only check breaches after idx_ts
            result = self._atr_trail_leg(result, ce, ce_sl, eod_ts, "ce",
                                          trail_start_ts=idx_ts)

            # CE hedge → close at EOD price regardless of how CE sell exited
            if not ceh.empty and result.ce_hedge_exit is None:
                ceh_at_eod = ceh[ceh.index <= eod_ts]
                if not ceh_at_eod.empty:
                    result.ce_hedge_exit        = float(ceh_at_eod["close"].iloc[-1])
                    result.ce_hedge_exit_reason = "EOD"

        return result

    def _atr_trail_leg(self, result, series, initial_sl, eod_ts, leg: str,
                       trail_start_ts=None) -> DayResult:
        """
        ATR trailing for the surviving sell leg after the other leg hit SL.

        Key design:
        - Uses FULL day series for ATR calculation (proper 5-min candle history)
        - Only checks for SL breaches AFTER trail_start_ts (when other leg hit SL)
        - ATR is on the configured timeframe (e.g. 5min), NOT on 1-second data
        - SL breach detection uses 1-second ticks within each candle window
        """
        if series.empty:
            _set_eod(result, series, leg)
            return result

        # Resample FULL series to ATR timeframe for proper historical ATR
        atr_candles = resample_to_timeframe(series, self.p.atr_timeframe)
        if atr_candles.empty:
            _set_eod(result, series, leg)
            return result

        current_sl = initial_sl

        for i in range(len(atr_candles)):
            candle_end   = atr_candles.index[i]
            candle_start = atr_candles.index[i - 1] if i > 0 else series.index[0]

            # Update ATR SL using all candles up to current (needs atr_period candles)
            if i >= self.p.atr_period:
                new_sl = compute_atr_trail_sl(
                    atr_candles.iloc[:i + 1],
                    self.p.atr_period,
                    self.p.atr_multiplier,
                    is_short=True
                )
                # Only tighten trailing SL (lower = tighter for short)
                if new_sl and new_sl < current_sl:
                    current_sl = new_sl

            # Only check for breaches AFTER trail_start_ts
            if trail_start_ts is not None and candle_end <= trail_start_ts:
                continue

            # Get 1-sec ticks within this candle (after trail start if needed)
            tick_start = max(candle_start,
                             trail_start_ts) if trail_start_ts else candle_start
            tick_window = series[
                (series.index > tick_start) & (series.index <= candle_end)
            ]

            if tick_window.empty:
                continue

            # Vectorized SL breach check on 1-sec ticks
            breach_idx = _first_breach_idx(tick_window, current_sl)
            if breach_idx is not None:
                breach_ts = tick_window.index[breach_idx]
                _set_sl_exit(result, current_sl, breach_ts, "ATR_TRAIL_SL", leg)
                return result

            # EOD check
            if candle_end >= eod_ts:
                _set_eod(result, series, leg)
                return result

        # Survived all candles → EOD exit
        _set_eod(result, series, leg)
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
    Step-wise hedge trailing using vectorized max-tracking.
    Scans each second, tracks running max, computes step SL.
    """
    import numpy as np
    if hedge_series.empty:
        return result

    closes = hedge_series["close"].values
    lows   = hedge_series["low"].values if "low" in hedge_series.columns else closes

    running_max = entry_price
    for i in range(len(closes)):
        if closes[i] > running_max:
            running_max = closes[i]
        sl = compute_hedge_step_sl(running_max, entry_price, step)
        if sl is not None and lows[i] <= sl:
            ts = hedge_series.index[i]
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
    if series.empty: return None
    mask = series.index.strftime("%H:%M") == time_str
    m    = series[mask]
    if m.empty:
        before = series[series.index.strftime("%H:%M") <= time_str]
        return float(before[col].iloc[-1]) if not before.empty else None
    return float(m[col].iloc[0])
