"""
config.py — Strategy parameters and backtesting grid configuration.
All monetary values in INR. All times in IST (HH:MM string).
"""

from dataclasses import dataclass, field
from typing import List


# ─────────────────────────────────────────────
#  SINGLE RUN PARAMETERS (defaults)
# ─────────────────────────────────────────────

@dataclass
class StrategyParams:
    # ── ATM Selection ──────────────────────────
    atm_scan_start: str   = "09:16"   # Start of ATM scan window
    atm_scan_end:   str   = "09:21"   # End of ATM scan window (inclusive)
    max_premium_diff: float = 20.0    # Max allowed |CE - PE| premium at ATM

    # ── Hedge ──────────────────────────────────
    hedge_pct: float = 0.05           # Hedge target = hedge_pct * sell_premium (e.g. 0.05 = 5%)

    # ── VIX Thresholds ─────────────────────────
    vix_low:               float = 12.0   # VIX < 12  → SL regime 1
    vix_mid_low:           float = 16.0   # 12 ≤ VIX < 16 → regime 2/3 (depends on intraday move)
    vix_mid_high:          float = 20.0   # 16 ≤ VIX < 20 → regime 4
                                           # VIX ≥ 20 → regime 5
    vix_intraday_threshold: float = 3.0   # % move from prev-close that triggers tighter SL in regime 2/3

    # ── Stop Loss Percentages ──────────────────
    # SL = sell_premium * sl_pct + sl_buffer, then rounded UP to nearest 5
    sl_pct_vix_lt12:        float = 0.40  # VIX < 12
    sl_pct_vix_12_16_calm:  float = 0.40  # VIX 12-16, intraday VIX move ≤ threshold
    sl_pct_vix_12_16_volatile: float = 0.25  # VIX 12-16, intraday VIX move > threshold
    sl_pct_vix_16_20:       float = 0.25  # VIX 16-20
    sl_pct_vix_gt20:        float = 0.15  # VIX > 20
    sl_buffer:              float = 5.0   # Flat buffer (rupees) added after % calc

    # ── ATR Trailing (surviving sell leg) ──────
    atr_timeframe:   str   = "5min"   # Candle size for ATR: "1min", "5min", "15min"
    atr_period:      int   = 14       # Lookback period for ATR
    atr_multiplier:  float = 1.5      # SL = candle_close + atr_multiplier * ATR

    # ── Hedge Step Trailing ────────────────────
    # When one sell leg hits SL, its hedge is in profit.
    # Trail: when hedge rises by `step` from last milestone, lock in previous milestone as SL.
    hedge_trail_step: float = 3.0     # Step size in rupees (default: 3)

    # ── Exit ───────────────────────────────────
    eod_exit_time: str = "15:20"      # Square off all positions at this time

    # ── Lot Size ───────────────────────────────
    lot_size: int = 75                # NIFTY lot size (verify current NSE lot size)

    # ── Expiry ─────────────────────────────────
    expiry_weekday: int = 3           # 0=Mon … 3=Thu (NIFTY weekly expiry = Thursday)

    def __str__(self):
        return (
            f"ATM[{self.atm_scan_start}-{self.atm_scan_end} maxdiff={self.max_premium_diff}] "
            f"Hedge={self.hedge_pct*100:.0f}% VIXthr={self.vix_intraday_threshold}% "
            f"ATR[{self.atr_timeframe},p{self.atr_period},x{self.atr_multiplier}] "
            f"Step={self.hedge_trail_step} EOD={self.eod_exit_time}"
        )

    def to_dict(self):
        return {
            "atm_scan_start": self.atm_scan_start,
            "atm_scan_end": self.atm_scan_end,
            "max_premium_diff": self.max_premium_diff,
            "hedge_pct": self.hedge_pct,
            "vix_intraday_threshold": self.vix_intraday_threshold,
            "atr_timeframe": self.atr_timeframe,
            "atr_period": self.atr_period,
            "atr_multiplier": self.atr_multiplier,
            "hedge_trail_step": self.hedge_trail_step,
            "eod_exit_time": self.eod_exit_time,
            "lot_size": self.lot_size,
        }


# ─────────────────────────────────────────────
#  GRID SEARCH CONFIG (ranges to iterate over)
# ─────────────────────────────────────────────

@dataclass
class GridConfig:
    """
    Each list = values to try for that parameter.
    Total combinations = product of all list lengths.
    """

    # ATM scan window
    atm_scan_starts:  List[str]   = field(default_factory=lambda: ["09:16", "09:17", "09:18"])
    atm_scan_ends:    List[str]   = field(default_factory=lambda: ["09:20", "09:21"])
    max_premium_diffs: List[float] = field(default_factory=lambda: [10.0, 20.0, 30.0])

    # Hedge
    hedge_pcts: List[float] = field(default_factory=lambda: [0.03, 0.05, 0.07])

    # VIX intraday trigger
    vix_intraday_thresholds: List[float] = field(default_factory=lambda: [2.0, 3.0, 4.0])

    # ATR trailing
    atr_timeframes:   List[str]   = field(default_factory=lambda: ["1min", "3min", "5min", "15min"])  # supported: 1min,3min,5min,15min,30min
    atr_periods:      List[int]   = field(default_factory=lambda: [7, 14, 21])
    atr_multipliers:  List[float] = field(default_factory=lambda: [1.0, 1.5, 2.0])

    # Hedge step trail
    hedge_trail_steps: List[float] = field(default_factory=lambda: [2.0, 3.0, 4.0])

    # EOD exit
    eod_exit_times: List[str] = field(default_factory=lambda: ["15:15", "15:20", "15:25"])

    def total_combinations(self) -> int:
        count = 1
        for lst in [
            self.atm_scan_starts, self.atm_scan_ends, self.max_premium_diffs,
            self.hedge_pcts, self.vix_intraday_thresholds,
            self.atr_timeframes, self.atr_periods, self.atr_multipliers,
            self.hedge_trail_steps, self.eod_exit_times,
        ]:
            count *= len(lst)
        return count


# ─────────────────────────────────────────────
#  DATA CONFIG
# ─────────────────────────────────────────────

@dataclass
class DataConfig:
    db_path: str = "nifty_backtest.db"          # SQLite database path
    nifty_spot_code:  str = "NIFTY"              # Breeze stock code for NIFTY
    vix_stock_code:   str = "INDIAVIX"           # Breeze stock code for India VIX
    exchange_code:    str = "NSE"
    options_exchange: str = "NFO"
    strike_range:     int = 1500                 # ± range around ATM to download (points)
    strike_step:      int = 50                   # NIFTY option strike spacing
    lot_size:         int = 75                   # Override per date if needed
