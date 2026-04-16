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

        result.atm_strike = atm_strike
        result.ce_entry   = ce_prem
        result.pe_entry   = pe_prem
        result.entry_time = entry_time

        # Phase 1b: Hedge selection
        ce_hedge_strike, ce_hedge_entry = find_hedge_strike_from_daydata(
            day, entry_time, atm_strike, ce_prem, self.p.hedge_pct, "CE"
        )
        pe_hedge_strike, pe_hedge_entry = find_hedge_strike_from_daydata(
            day, entry_time, atm_strike, pe_prem, self.p.hedge_pct, "PE"
        )

        result.ce_hedge_strike = ce_hedge_strike
        result.ce_hedge_entry  = ce_hedge_entry
        result.pe_hedge_strike = pe_hedge_strike
        result.pe_hedge_entry  = pe_hedge_entry

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
        eod_ts = pd.Timestamp(f"{result.date} {self.p.eod_exit_time}")

        ce_open  = True;  pe_open  = True
        ceh_open = not ceh_series.empty
        peh_open = not peh_series.empty

        ce_sl = result.ce_sl;  pe_sl = result.pe_sl
        ce_in_trail = False;   pe_in_trail = False
        ce_atr_sl = ce_sl;     pe_atr_sl = pe_sl

        ce_hedge_max = result.ce_hedge_entry or 0.0
        pe_hedge_max = result.pe_hedge_entry or 0.0

        ce_buf: List[dict] = []
        pe_buf: List[dict] = []

        all_times = _build_timeline(ce_series, entry_time, eod_ts)

        for ts in all_times:
            is_eod   = (ts >= eod_ts)
            time_str = ts.strftime("%H:%M")

            ce_close  = _get_close(ce_series,  ts)
            pe_close  = _get_close(pe_series,  ts)
            ce_high   = _get_high(ce_series,   ts)
            pe_high   = _get_high(pe_series,   ts)
            ceh_close = _get_close(ceh_series, ts) if ceh_open else None
            peh_close = _get_close(peh_series, ts) if peh_open else None
            ceh_low   = _get_low(ceh_series,   ts) if ceh_open else None
            peh_low   = _get_low(peh_series,   ts) if peh_open else None

            # EOD square-off
            if is_eod:
                if ce_open and ce_close:
                    result.ce_exit = ce_close; result.ce_exit_reason = "EOD"
                    result.ce_exit_time = time_str; ce_open = False
                if pe_open and pe_close:
                    result.pe_exit = pe_close; result.pe_exit_reason = "EOD"
                    result.pe_exit_time = time_str; pe_open = False
                if ceh_open and ceh_close:
                    result.ce_hedge_exit = ceh_close
                    result.ce_hedge_exit_reason = "EOD"; ceh_open = False
                if peh_open and peh_close:
                    result.pe_hedge_exit = peh_close
                    result.pe_hedge_exit_reason = "EOD"; peh_open = False
                break

            # Candle buffers
            c = _get_candle(ce_series, ts)
            if c: ce_buf.append(c)
            c = _get_candle(pe_series, ts)
            if c: pe_buf.append(c)

            # CE sell SL check
            if ce_open and ce_high is not None:
                active_sl = ce_atr_sl if ce_in_trail else ce_sl
                if active_sl and ce_high >= active_sl:
                    result.ce_exit = active_sl
                    result.ce_exit_reason = "ATR_TRAIL_SL" if ce_in_trail else "FIXED_SL"
                    result.ce_exit_time = time_str; ce_open = False
                    if not pe_in_trail:
                        pe_in_trail = True
                        sl = self._compute_atr_sl(pe_buf)
                        pe_atr_sl = sl if sl else pe_sl

            # PE sell SL check
            if pe_open and pe_high is not None:
                active_sl = pe_atr_sl if pe_in_trail else pe_sl
                if active_sl and pe_high >= active_sl:
                    result.pe_exit = active_sl
                    result.pe_exit_reason = "ATR_TRAIL_SL" if pe_in_trail else "FIXED_SL"
                    result.pe_exit_time = time_str; pe_open = False
                    if not ce_in_trail:
                        ce_in_trail = True
                        sl = self._compute_atr_sl(ce_buf)
                        ce_atr_sl = sl if sl else ce_sl

            # Update ATR trailing SL
            if ce_open and ce_in_trail:
                sl = self._compute_atr_sl(ce_buf)
                if sl and sl < ce_atr_sl: ce_atr_sl = sl

            if pe_open and pe_in_trail:
                sl = self._compute_atr_sl(pe_buf)
                if sl and sl < pe_atr_sl: pe_atr_sl = sl

            # CE hedge step trailing (after CE sell hits SL)
            if ceh_open and ceh_close and not ce_open:
                if ceh_close > ce_hedge_max: ce_hedge_max = ceh_close
                sl = compute_hedge_step_sl(ce_hedge_max, result.ce_hedge_entry or 0.0,
                                           self.p.hedge_trail_step)
                if sl and ceh_low is not None and ceh_low <= sl:
                    result.ce_hedge_exit = sl
                    result.ce_hedge_exit_reason = "STEP_TRAIL_SL"; ceh_open = False

            # PE hedge step trailing (after PE sell hits SL)
            if peh_open and peh_close and not pe_open:
                if peh_close > pe_hedge_max: pe_hedge_max = peh_close
                sl = compute_hedge_step_sl(pe_hedge_max, result.pe_hedge_entry or 0.0,
                                           self.p.hedge_trail_step)
                if sl and peh_low is not None and peh_low <= sl:
                    result.pe_hedge_exit = sl
                    result.pe_hedge_exit_reason = "STEP_TRAIL_SL"; peh_open = False

            if not ce_open and not pe_open and not ceh_open and not peh_open:
                break

        return result

    def _compute_atr_sl(self, buf: list) -> Optional[float]:
        if len(buf) < self.p.atr_period + 2:
            return None
        df = pd.DataFrame(buf).set_index("datetime")
        df = resample_to_timeframe(df, self.p.atr_timeframe)
        if len(df) < self.p.atr_period + 2:
            return None
        return compute_atr_trail_sl(df, self.p.atr_period, self.p.atr_multiplier, is_short=True)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_timeline(series, entry_time, eod_ts):
    if series.empty: return []
    date_str = series.index[0].strftime("%Y-%m-%d")
    entry_ts = pd.Timestamp(f"{date_str} {entry_time}")
    mask = (series.index >= entry_ts) & (series.index <= eod_ts)
    return list(series.index[mask])

def _get_close(series, ts):
    if series.empty or ts not in series.index: return None
    v = series.at[ts, "close"]
    return float(v) if pd.notna(v) and v > 0 else None

def _get_high(series, ts):
    if series.empty or ts not in series.index: return None
    v = series.at[ts, "high"]
    return float(v) if pd.notna(v) and v > 0 else None

def _get_low(series, ts):
    if series.empty or ts not in series.index: return None
    v = series.at[ts, "low"]
    return float(v) if pd.notna(v) and v > 0 else None

def _get_candle(series, ts):
    if series.empty or ts not in series.index: return None
    row = series.loc[ts]
    return {"datetime": ts, "open": float(row.get("open", row["close"])),
            "high": float(row.get("high", row["close"])),
            "low": float(row.get("low",  row["close"])),
            "close": float(row["close"])}

def _get_value_at_time(series, time_str, col="close"):
    if series.empty: return None
    mask = series.index.strftime("%H:%M") == time_str
    m = series[mask]
    if m.empty:
        before = series[series.index.strftime("%H:%M") <= time_str]
        return float(before[col].iloc[-1]) if not before.empty else None
    return float(m[col].iloc[0])
