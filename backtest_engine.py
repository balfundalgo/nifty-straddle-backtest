"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    BALFUND NIFTY OPTIONS STRADDLE BACKTESTER                ║
║                           Version 1.0 · May 2026                          ║
║                                                                            ║
║  Short Straddle with OTM Hedges · VIX-Based SL · ATR Trailing · Re-entry  ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import sys
import time
import glob
import json
import math
import logging
import argparse
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time as dtime
from typing import Optional, Dict, List, Tuple
from pathlib import Path
from enum import Enum

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich import box

warnings.filterwarnings("ignore")
console = Console()

# ─── NIFTY LOT SIZE HISTORY ────────────────────────────────────────────────────
# Based on NSE circulars — for backtesting from 2020 onwards
# Period                  | Lot Size | Source
# 2007-2015 (approx)     | 25       | (pre-2015 era)
# ~Nov 2014 → Apr 2024   | 50       | NSE periodic revision; Nifty was 50 through 2020-2024
# Apr 26 2024 → Nov 2024 | 25       | NSE halved from 50 to 25 (circular Apr 2, 2024)
# Nov 20 2024 → Dec 2025 | 75       | SEBI min contract ₹15L rule (circular Oct 18, 2024)
# Jan 2026 onwards        | 65       | NSE revision effective Dec 30, 2025 EOD
#
# NOTE: The options data from 2020 sample shows volume=75 which was the trading
# quantity, not the lot size. Nifty lot size was 50 from 2020 to April 2024.
# BUT — looking at the data more carefully and cross-referencing with known lot sizes,
# the lot size was actually 75 from ~Nov 2014 until a mid-period change.
# Let me use the most accurate timeline:
# Actually from the search results: Nifty was at 50 in 2023 (confirmed by multiple sources).
# The Zerodha thread says 2007-2015 was 25, then 2015-2018 was 75.
# But NSE changed it several times. The ICICIdirect article from 2023 confirms:
# "Nifty50 ... unchanged at 50" — so 50 was the lot size in 2023.
# The Apr 2024 circular halved it from 50 to 25.
#
# Most accurate timeline for 2020+:
# 2020-01-01 to 2024-04-25 : 50
# 2024-04-26 to 2024-11-19 : 25
# 2024-11-20 to 2025-12-30 : 75
# 2025-12-31 onwards        : 65

LOT_SIZE_HISTORY = [
    # (start_date_inclusive, lot_size)
    (datetime(2000, 1, 1),  50),   # Default for early period
    (datetime(2024, 4, 26), 25),   # NSE halved 50 → 25
    (datetime(2024, 11, 20), 75),  # SEBI ₹15L min → 25 → 75
    (datetime(2025, 12, 31), 65),  # NSE revision 75 → 65
]


def get_lot_size(date: datetime) -> int:
    """Return NIFTY lot size for a given date."""
    lot = 50  # default
    for start, size in LOT_SIZE_HISTORY:
        if date >= start:
            lot = size
    return lot


# ─── CONFIGURATION ──────────────────────────────────────────────────────────────

@dataclass
class VIXRange:
    """A VIX range mapped to a stop-loss percentage."""
    vix_low: float
    vix_high: float
    sl_pct: float  # stop loss as % of sold premium (e.g., 50 means SL at 1.5x)

    def __repr__(self):
        return f"VIX[{self.vix_low}-{self.vix_high}] → SL={self.sl_pct}%"


@dataclass
class BacktestConfig:
    """Full configuration for the backtest."""

    # ── Data Paths ──
    spot_data_dir: str = ""          # Directory with NIFTY spot CSV/Parquet files
    vix_data_dir: str = ""           # Directory with INDVIX CSV/Parquet files
    options_data_dir: str = ""       # Directory with options CSV/Parquet files
    output_dir: str = "results"      # Output directory for results

    # ── Date Range ──
    start_date: str = "2020-01-01"
    end_date: str = "2026-05-11"

    # ── Strike Selection ──
    scan_start_time: str = "09:16:00"    # Start scanning for equal premiums
    scan_end_time: str = "09:18:00"      # Deadline — force sell at this time
    nifty_strike_interval: int = 50      # NIFTY strike interval (50 points)

    # ── Hedge ──
    hedge_multiplier: float = 1.5        # Total sold premium × this = hedge distance in points

    # ── VIX-Based Stop Loss ──
    vix_ranges: List[VIXRange] = field(default_factory=lambda: [
        VIXRange(0, 12, 40),     # Low VIX: SL at 40% of premium
        VIXRange(12, 16, 50),    # Medium VIX: SL at 50%
        VIXRange(16, 20, 60),    # High VIX: SL at 60%
        VIXRange(20, 30, 80),    # Very High VIX: SL at 80%
        VIXRange(30, 100, 100),  # Extreme VIX: SL at 100%
    ])

    # ── Intraday VIX Spike (toggleable) ──
    vix_spike_enabled: bool = True
    vix_spike_threshold_pct: float = 3.0   # y% spike threshold
    vix_spike_new_sl_pct: float = 30.0     # z% — new SL after spike

    # ── Target / Exit ──
    exit_time: str = "14:45:00"            # Square off time if no SL hit

    # ── ATR Trailing (after first SL hit) ──
    # Surviving sold leg: 5-min ATR
    atr_sold_timeframe_minutes: int = 5
    atr_sold_period: int = 21
    atr_sold_multiplier: float = 2.0
    # Stopped-out leg's hedge: 1-min ATR
    atr_hedge_timeframe_minutes: int = 1
    atr_hedge_period: int = 21
    atr_hedge_multiplier: float = 3.0

    # ── Trade Management ──
    max_trades_per_day: int = 1
    max_daily_loss_pct: float = 2.0        # % of capital
    capital: float = 1000000.0             # ₹10 Lakh default
    num_lots: int = 1
    slippage_pct: float = 0.001            # 0.001% slippage

    # ── Misc ──
    convert_to_parquet: bool = True        # Convert CSVs to Parquet for speed
    log_level: str = "INFO"

    def get_exit_time(self) -> dtime:
        parts = self.exit_time.split(":")
        return dtime(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0)

    def get_scan_start(self) -> dtime:
        parts = self.scan_start_time.split(":")
        return dtime(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0)

    def get_scan_end(self) -> dtime:
        parts = self.scan_end_time.split(":")
        return dtime(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0)


# ─── ENUMS & DATA CLASSES ───────────────────────────────────────────────────────

class LegStatus(Enum):
    ACTIVE = "ACTIVE"
    SL_HIT = "SL_HIT"
    TRAILING = "TRAILING"
    TRAIL_SL_HIT = "TRAIL_SL_HIT"
    TARGET_EXIT = "TARGET_EXIT"
    DAILY_LOSS_EXIT = "DAILY_LOSS_EXIT"


@dataclass
class TradeLeg:
    """Represents a single option leg (sold or hedge)."""
    leg_type: str              # "SOLD_CE", "SOLD_PE", "HEDGE_CE", "HEDGE_PE"
    strike: float
    option_type: str           # "Call" or "Put"
    entry_price: float
    entry_time: datetime
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    status: LegStatus = LegStatus.ACTIVE
    sl_price: Optional[float] = None
    trailing_sl: Optional[float] = None
    pnl: float = 0.0

    @property
    def is_sold(self) -> bool:
        return self.leg_type.startswith("SOLD")

    @property
    def is_hedge(self) -> bool:
        return self.leg_type.startswith("HEDGE")


@dataclass
class TradeGroup:
    """A complete trade: sold CE + sold PE + hedge CE + hedge PE."""
    trade_id: int
    date: datetime
    legs: Dict[str, TradeLeg] = field(default_factory=dict)
    total_premium_received: float = 0.0
    atm_strike: float = 0.0
    vix_at_entry: float = 0.0
    sl_pct: float = 0.0
    first_sl_leg: Optional[str] = None   # which leg hit SL first
    total_pnl: float = 0.0
    exit_reason: str = ""
    lot_size: int = 50
    num_lots: int = 1


@dataclass
class DayResult:
    """Results for a single trading day."""
    date: datetime
    trades: List[TradeGroup] = field(default_factory=list)
    daily_pnl: float = 0.0
    vix_open: float = 0.0
    vix_high: float = 0.0
    vix_low: float = 0.0
    spot_open: float = 0.0
    spot_close: float = 0.0


# ─── DATA LOADER ────────────────────────────────────────────────────────────────

class DataLoader:
    """Handles loading and caching of spot, VIX, and options data."""

    def __init__(self, config: BacktestConfig):
        self.config = config
        self.parquet_cache_dir = os.path.join(config.output_dir, "_parquet_cache")
        os.makedirs(self.parquet_cache_dir, exist_ok=True)

        # Cache of expiry folders (sorted dates)
        self._expiry_folders: Optional[List[str]] = None

    def _csv_to_parquet(self, csv_path: str) -> str:
        """Convert CSV to Parquet for faster reads. Returns Parquet path."""
        basename = Path(csv_path).stem + ".parquet"
        # Determine subfolder based on parent dir name
        parent_name = Path(csv_path).parent.name
        cache_subdir = os.path.join(self.parquet_cache_dir, parent_name)
        os.makedirs(cache_subdir, exist_ok=True)
        pq_path = os.path.join(cache_subdir, basename)

        if os.path.exists(pq_path):
            # Check if parquet is newer than CSV
            if os.path.getmtime(pq_path) >= os.path.getmtime(csv_path):
                return pq_path

        df = pd.read_csv(csv_path)
        df["datetime"] = pd.to_datetime(df["datetime"])
        table = pa.Table.from_pandas(df)
        pq.write_table(table, pq_path, compression="snappy")
        return pq_path

    def preconvert_all_csvs(self, dates: List[str]):
        """
        Phase 1: Convert ALL relevant CSVs to Parquet upfront.
        This runs once before the backtest so all reads are fast.
        """
        # Collect all CSV files to convert
        csv_files = []

        for date_str in dates:
            # Spot
            spot_csv = os.path.join(self.config.spot_data_dir, f"{date_str}.csv")
            if os.path.exists(spot_csv):
                csv_files.append(spot_csv)

            # VIX
            vix_csv = os.path.join(self.config.vix_data_dir, f"{date_str}.csv")
            if os.path.exists(vix_csv):
                csv_files.append(vix_csv)

            # Options: find expiry folder for this date, glob all CSVs for this trading date
            expiry = self._find_expiry_for_date(date_str)
            if expiry:
                expiry_dir = os.path.join(self.config.options_data_dir, expiry)
                if os.path.isdir(expiry_dir):
                    pattern = os.path.join(expiry_dir, f"{date_str}_*.csv")
                    csv_files.extend(glob.glob(pattern))

            # Also check flat layout
            flat_pattern = os.path.join(self.config.options_data_dir, f"{date_str}_*.csv")
            csv_files.extend(glob.glob(flat_pattern))

        # Deduplicate
        csv_files = sorted(set(csv_files))

        if not csv_files:
            console.print("[yellow]No CSV files found to convert.[/yellow]")
            return

        # Filter out already-converted files (parquet exists and is newer)
        to_convert = []
        for csv_path in csv_files:
            basename = Path(csv_path).stem + ".parquet"
            parent_name = Path(csv_path).parent.name
            cache_subdir = os.path.join(self.parquet_cache_dir, parent_name)
            pq_path = os.path.join(cache_subdir, basename)
            if os.path.exists(pq_path) and os.path.getmtime(pq_path) >= os.path.getmtime(csv_path):
                continue  # Already converted
            to_convert.append(csv_path)

        if not to_convert:
            console.print(f"[green]✓ All {len(csv_files)} files already in Parquet cache.[/green]")
            return

        console.print(f"[cyan]Converting {len(to_convert)} CSVs to Parquet "
                       f"({len(csv_files) - len(to_convert)} already cached)...[/cyan]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("•"),
            TextColumn("[cyan]{task.completed}/{task.total} files"),
            TextColumn("•"),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("CSV → Parquet", total=len(to_convert))
            for csv_path in to_convert:
                fname = Path(csv_path).name
                progress.update(task, description=f"Converting {fname}")
                try:
                    self._csv_to_parquet(csv_path)
                except Exception as e:
                    console.print(f"[red]  Failed: {fname}: {e}[/red]")
                progress.advance(task)

        console.print(f"[green]✓ Parquet conversion complete. Cache: {self.parquet_cache_dir}[/green]\n")

    def load_spot_data(self, date_str: str) -> Optional[pd.DataFrame]:
        """Load NIFTY spot data for a given date."""
        csv_path = os.path.join(self.config.spot_data_dir, f"{date_str}.csv")
        if not os.path.exists(csv_path):
            return None

        if self.config.convert_to_parquet:
            pq_path = self._csv_to_parquet(csv_path)
            df = pd.read_parquet(pq_path)
        else:
            df = pd.read_csv(csv_path)
            df["datetime"] = pd.to_datetime(df["datetime"])

        df = df.sort_values("datetime").reset_index(drop=True)
        return df

    def load_vix_data(self, date_str: str) -> Optional[pd.DataFrame]:
        """Load INDVIX data for a given date."""
        csv_path = os.path.join(self.config.vix_data_dir, f"{date_str}.csv")
        if not os.path.exists(csv_path):
            return None

        if self.config.convert_to_parquet:
            pq_path = self._csv_to_parquet(csv_path)
            df = pd.read_parquet(pq_path)
        else:
            df = pd.read_csv(csv_path)
            df["datetime"] = pd.to_datetime(df["datetime"])

        df = df.sort_values("datetime").reset_index(drop=True)
        return df

    def _get_expiry_folders(self) -> List[str]:
        """
        Scan options_data_dir for expiry date subfolders.
        Returns sorted list of expiry date strings (YYYY-MM-DD).
        Cached after first call.
        """
        if self._expiry_folders is not None:
            return self._expiry_folders

        expiries = []
        if not os.path.isdir(self.config.options_data_dir):
            self._expiry_folders = []
            return []

        for entry in os.listdir(self.config.options_data_dir):
            full_path = os.path.join(self.config.options_data_dir, entry)
            if os.path.isdir(full_path):
                try:
                    datetime.strptime(entry, "%Y-%m-%d")
                    expiries.append(entry)
                except ValueError:
                    pass

        self._expiry_folders = sorted(expiries)
        return self._expiry_folders

    def _find_expiry_for_date(self, trading_date_str: str) -> Optional[str]:
        """
        Find the correct expiry folder for a given trading date.
        The expiry is the nearest expiry date >= trading date.
        (You trade on Monday-Thursday, expiry is on Thursday of that week.)
        """
        expiries = self._get_expiry_folders()
        if not expiries:
            return None

        for expiry in expiries:
            if expiry >= trading_date_str:
                return expiry

        return None

    def _find_option_csv(self, date_str: str, strike_str: str, opt_type: str) -> Optional[str]:
        """
        Find option CSV file. Layout:
          options_dir/{expiry_date}/{trading_date}_{strike}_{CE/PE}.csv

        Expiry folders are weekly expiry dates. Files inside are named by
        trading date. For a given trading_date, we look in the nearest
        expiry folder >= trading_date.

        Also supports flat layout as fallback:
          options_dir/{trading_date}_{strike}_{CE/PE}.csv
        """
        filename = f"{date_str}_{strike_str}_{opt_type}.csv"

        # Primary: expiry subfolder structure
        expiry = self._find_expiry_for_date(date_str)
        if expiry is not None:
            path_sub = os.path.join(self.config.options_data_dir, expiry, filename)
            if os.path.exists(path_sub):
                return path_sub

        # Fallback: flat layout
        path_flat = os.path.join(self.config.options_data_dir, filename)
        if os.path.exists(path_flat):
            return path_flat

        return None

    def load_option_data(self, date_str: str, strike: float, opt_type: str) -> Optional[pd.DataFrame]:
        """
        Load options data for a specific trading date/strike/type.
        opt_type: 'CE' or 'PE'
        Automatically finds the correct expiry folder.
        """
        # Handle strike formatting — could be int or float
        strike_str = str(int(strike)) if strike == int(strike) else str(strike)
        csv_path = self._find_option_csv(date_str, strike_str, opt_type)

        if csv_path is None:
            # Try with .0 suffix
            strike_str_f = f"{strike:.1f}"
            csv_path = self._find_option_csv(date_str, strike_str_f, opt_type)

        if csv_path is None:
            return None

        if self.config.convert_to_parquet:
            pq_path = self._csv_to_parquet(csv_path)
            df = pd.read_parquet(pq_path)
        else:
            df = pd.read_csv(csv_path)
            df["datetime"] = pd.to_datetime(df["datetime"])

        df = df.sort_values("datetime").reset_index(drop=True)
        return df

    def get_available_strikes(self, date_str: str) -> Dict[str, List[float]]:
        """
        Scan for available strikes on a given trading date.
        Looks inside the correct expiry folder for files named {date_str}_*_{CE/PE}.csv
        """
        ce_strikes = []
        pe_strikes = []

        # Determine which directories to search
        search_dirs = []

        # Primary: expiry subfolder
        expiry = self._find_expiry_for_date(date_str)
        if expiry is not None:
            expiry_dir = os.path.join(self.config.options_data_dir, expiry)
            if os.path.isdir(expiry_dir):
                search_dirs.append(expiry_dir)

        # Fallback: flat directory
        search_dirs.append(self.config.options_data_dir)

        for search_dir in search_dirs:
            for opt_type, strike_list in [("CE", ce_strikes), ("PE", pe_strikes)]:
                pattern = os.path.join(search_dir, f"{date_str}_*_{opt_type}.csv")
                for f in glob.glob(pattern):
                    basename = Path(f).stem
                    # Filename: {YYYY-MM-DD}_{strike}_{CE/PE}
                    # Split by _: ['2020', '09', '10', '12150', 'CE']
                    parts = basename.split("_")
                    try:
                        strike = float(parts[-2])
                        if strike not in strike_list:
                            strike_list.append(strike)
                    except (IndexError, ValueError):
                        pass

        return {"CE": sorted(ce_strikes), "PE": sorted(pe_strikes)}

    def get_available_dates(self) -> List[str]:
        """Get all dates for which spot data is available."""
        dates = set()
        for f in glob.glob(os.path.join(self.config.spot_data_dir, "*.csv")):
            basename = Path(f).stem  # e.g., "2026-05-05"
            # Validate it looks like a date
            try:
                datetime.strptime(basename, "%Y-%m-%d")
                dates.add(basename)
            except ValueError:
                pass

        # Also check parquet
        for f in glob.glob(os.path.join(self.config.spot_data_dir, "*.parquet")):
            basename = Path(f).stem
            try:
                datetime.strptime(basename, "%Y-%m-%d")
                dates.add(basename)
            except ValueError:
                pass

        return sorted(dates)


# ─── ATR CALCULATOR ─────────────────────────────────────────────────────────────

class ATRCalculator:
    """Computes ATR-based trailing stop from tick data."""

    def __init__(self, timeframe_minutes: int, period: int, multiplier: float):
        self.timeframe_minutes = timeframe_minutes
        self.period = period
        self.multiplier = multiplier
        self.candles: List[Dict] = []
        self.atr_values: List[float] = []
        self._current_candle: Optional[Dict] = None
        self._candle_start: Optional[datetime] = None

    def reset(self):
        self.candles = []
        self.atr_values = []
        self._current_candle = None
        self._candle_start = None

    def _get_candle_start(self, ts: datetime) -> datetime:
        """Align timestamp to candle boundary."""
        minutes = ts.hour * 60 + ts.minute
        candle_start_min = (minutes // self.timeframe_minutes) * self.timeframe_minutes
        return ts.replace(
            hour=candle_start_min // 60,
            minute=candle_start_min % 60,
            second=0, microsecond=0
        )

    def update(self, ts: datetime, price: float) -> Optional[float]:
        """
        Feed a tick. Returns trailing SL if enough candles exist, else None.
        For a SOLD leg (short), trailing SL is ABOVE current price.
        This method returns the raw ATR trailing value — caller decides direction.
        """
        candle_start = self._get_candle_start(ts)

        if self._candle_start is None or candle_start > self._candle_start:
            # Close previous candle
            if self._current_candle is not None:
                self.candles.append(self._current_candle.copy())
                self._compute_atr()

            # Start new candle
            self._candle_start = candle_start
            self._current_candle = {
                "open": price, "high": price, "low": price, "close": price, "time": candle_start
            }
        else:
            # Update current candle
            if self._current_candle is not None:
                self._current_candle["high"] = max(self._current_candle["high"], price)
                self._current_candle["low"] = min(self._current_candle["low"], price)
                self._current_candle["close"] = price

        # Return trailing SL based on latest closed candle
        if len(self.atr_values) > 0 and len(self.candles) > 0:
            last_close = self.candles[-1]["close"]
            atr = self.atr_values[-1]
            return atr  # Return raw ATR, caller computes SL direction
        return None

    def _compute_atr(self):
        """Compute Wilder's RMA-based ATR from candles."""
        if len(self.candles) < 2:
            self.atr_values = []
            return

        # True Range
        trs = []
        for i in range(1, len(self.candles)):
            c = self.candles[i]
            pc = self.candles[i - 1]
            tr = max(
                c["high"] - c["low"],
                abs(c["high"] - pc["close"]),
                abs(c["low"] - pc["close"])
            )
            trs.append(tr)

        if len(trs) < self.period:
            # Not enough data for full ATR, use SMA of available
            avg = sum(trs) / len(trs) if trs else 0
            self.atr_values = [avg]
            return

        # Wilder's RMA: first value is SMA, then EMA-like
        atr_vals = []
        sma = sum(trs[:self.period]) / self.period
        atr_vals.append(sma)
        for i in range(self.period, len(trs)):
            rma = (atr_vals[-1] * (self.period - 1) + trs[i]) / self.period
            atr_vals.append(rma)
        self.atr_values = atr_vals

    def get_trailing_sl_for_short(self) -> Optional[float]:
        """For a short (sold) option: SL is above → close + ATR * multiplier."""
        if len(self.atr_values) > 0 and len(self.candles) > 0:
            last_close = self.candles[-1]["close"]
            atr = self.atr_values[-1]
            return last_close + atr * self.multiplier
        return None

    def get_trailing_sl_for_long(self) -> Optional[float]:
        """For a long (bought hedge) option: SL is below → close - ATR * multiplier."""
        if len(self.atr_values) > 0 and len(self.candles) > 0:
            last_close = self.candles[-1]["close"]
            atr = self.atr_values[-1]
            return last_close - atr * self.multiplier
        return None


# ─── MAIN BACKTEST ENGINE ───────────────────────────────────────────────────────

class StraddleBacktester:
    """Core backtesting engine."""

    def __init__(self, config: BacktestConfig):
        self.config = config
        self.loader = DataLoader(config)
        self.results: List[DayResult] = []
        self.trade_log: List[TradeGroup] = []
        self.trade_counter = 0

        # Setup logging
        logging.basicConfig(
            level=getattr(logging, config.log_level),
            format="%(asctime)s | %(levelname)-7s | %(message)s",
            datefmt="%H:%M:%S",
        )
        self.logger = logging.getLogger("StraddleBacktest")

    def apply_slippage(self, price: float, is_buy: bool) -> float:
        """Apply slippage: buy higher, sell lower."""
        slip = price * (self.config.slippage_pct / 100)
        return price + slip if is_buy else price - slip

    @staticmethod
    def round_sl_up(raw_sl: float) -> float:
        """Round SL to the next multiple of 5 strictly above raw_sl.
        280.00 → 285, 280.01 → 285, 275.01 → 280, 444.92 → 445
        """
        return (int(raw_sl) // 5 + 1) * 5

    def get_sl_pct_for_vix(self, vix: float) -> float:
        """Get stop-loss percentage based on VIX value."""
        for vr in self.config.vix_ranges:
            if vr.vix_low <= vix < vr.vix_high:
                return vr.sl_pct
        # Default: last range
        return self.config.vix_ranges[-1].sl_pct if self.config.vix_ranges else 50.0

    def find_atm_strike(self, spot_price: float) -> float:
        """Find nearest ATM strike given spot price."""
        interval = self.config.nifty_strike_interval
        return round(spot_price / interval) * interval

    def _get_price_at_time(self, df: pd.DataFrame, target_time: datetime) -> Optional[float]:
        """Get the closest price at or before target_time."""
        mask = df["datetime"] <= target_time
        subset = df[mask]
        if subset.empty:
            return None
        return subset.iloc[-1]["close"]

    def _get_price_at_or_after(self, df: pd.DataFrame, target_time: datetime) -> Optional[Tuple[float, datetime]]:
        """Get closest price at or after target_time."""
        mask = df["datetime"] >= target_time
        subset = df[mask]
        if subset.empty:
            return None
        row = subset.iloc[0]
        return (row["close"], row["datetime"])

    def _resample_to_seconds(self, df: pd.DataFrame, date: datetime) -> pd.DataFrame:
        """
        Resample tick data to 1-second bars using forward-fill.
        This gives us a price for every second of the trading day.
        """
        if df.empty:
            return df

        # Create 1-second index for the full trading day
        start = datetime.combine(date.date(), dtime(9, 15, 0))
        end = datetime.combine(date.date(), dtime(15, 30, 0))
        idx = pd.date_range(start=start, end=end, freq="1s")

        # Set datetime as index and reindex
        df_sec = df.set_index("datetime")
        df_sec = df_sec.reindex(idx, method="ffill")
        df_sec.index.name = "datetime"
        df_sec = df_sec.reset_index()
        df_sec = df_sec.dropna(subset=["close"])
        return df_sec

    def select_strikes(
        self, date: datetime, spot_df: pd.DataFrame, available_strikes: Dict
    ) -> Optional[Tuple[float, float, float, float, datetime]]:
        """
        Select CE and PE strikes for the short straddle.

        Logic: We want ATM or near-ATM strikes where CE and PE premiums are
        as close as possible. We always sell the SAME strike for CE and PE
        (true straddle), or adjacent strikes if same strike doesn't have
        matching premiums (effectively a strangle with near-ATM strikes).

        Priority:
          1) Same strike (ATM) with closest premiums → true straddle
          2) ATM ± 1 strike apart with closest premiums → tight strangle
          3) At 09:18 deadline, force sell ATM CE + ATM PE regardless

        Returns: (ce_strike, pe_strike, ce_premium, pe_premium, entry_time) or None
        """
        scan_start = datetime.combine(date.date(), self.config.get_scan_start())
        scan_end = datetime.combine(date.date(), self.config.get_scan_end())

        # Get spot price around scan start
        spot_at_start = self._get_price_at_time(spot_df, scan_start)
        if spot_at_start is None:
            if not spot_df.empty:
                spot_at_start = spot_df.iloc[0]["close"]
            else:
                return None

        atm = self.find_atm_strike(spot_at_start)
        interval = self.config.nifty_strike_interval

        # Candidate strikes: ATM, ATM±1 only (tight near-ATM)
        candidate_strikes = [atm + i * interval for i in range(-1, 2)]  # ATM-50, ATM, ATM+50

        # Filter to available
        ce_candidates = [s for s in candidate_strikes if s in available_strikes.get("CE", [])]
        pe_candidates = [s for s in candidate_strikes if s in available_strikes.get("PE", [])]

        if not ce_candidates or not pe_candidates:
            self.logger.warning(f"  No CE/PE strikes available near ATM={atm}")
            return None

        # Load option data for candidates
        date_str = date.strftime("%Y-%m-%d")
        ce_data = {}
        pe_data = {}

        for s in ce_candidates:
            df = self.loader.load_option_data(date_str, s, "CE")
            if df is not None and not df.empty:
                ce_data[s] = df

        for s in pe_candidates:
            df = self.loader.load_option_data(date_str, s, "PE")
            if df is not None and not df.empty:
                pe_data[s] = df

        if not ce_data or not pe_data:
            self.logger.warning(f"  No option data loaded for candidates near ATM={atm}")
            return None

        # Build strike pairs to check, prioritized:
        # 1) Same strike pairs (true straddle) — sorted by distance from ATM
        # 2) Adjacent strike pairs (tight strangle) — only if same strike has no data
        strike_pairs = []
        common = sorted(set(ce_data.keys()) & set(pe_data.keys()), key=lambda s: abs(s - atm))
        for s in common:
            strike_pairs.append((s, s))  # Same strike CE+PE

        # Adjacent pairs only if no common strikes, or as fallback
        for ce_s in sorted(ce_data.keys(), key=lambda s: abs(s - atm)):
            for pe_s in sorted(pe_data.keys(), key=lambda s: abs(s - atm)):
                if (ce_s, pe_s) not in strike_pairs:
                    strike_pairs.append((ce_s, pe_s))

        # Scan second by second
        best_pair = None
        best_score = float("inf")

        current = scan_start
        while current <= scan_end:
            for ce_strike, pe_strike in strike_pairs:
                ce_df = ce_data[ce_strike]
                pe_df = pe_data[pe_strike]

                # Get CE price: latest tick at or before current time
                ce_before = ce_df[ce_df["datetime"] <= current]
                if ce_before.empty:
                    continue
                ce_price = ce_before.iloc[-1]["close"]
                if ce_price <= 0:
                    continue

                # Get PE price
                pe_before = pe_df[pe_df["datetime"] <= current]
                if pe_before.empty:
                    continue
                pe_price = pe_before.iloc[-1]["close"]
                if pe_price <= 0:
                    continue

                premium_diff = abs(ce_price - pe_price)

                # Score: premium difference is primary, ATM distance is tiebreaker
                # Same-strike pairs get a bonus (lower score)
                atm_dist = abs(ce_strike - atm) + abs(pe_strike - atm)
                same_strike_bonus = 0 if ce_strike == pe_strike else 1000

                score = premium_diff + same_strike_bonus + atm_dist * 0.0001

                if score < best_score:
                    best_score = score
                    best_pair = (ce_strike, pe_strike, ce_price, pe_price, current)

                # Perfect match on same strike — take it immediately
                if premium_diff == 0 and ce_strike == pe_strike:
                    return best_pair

            current += timedelta(seconds=1)

        # Return best found, or force ATM at scan_end
        if best_pair is not None:
            return best_pair

        # Fallback: force ATM at scan_end
        if atm in ce_data and atm in pe_data:
            ce_before = ce_data[atm][ce_data[atm]["datetime"] <= scan_end]
            pe_before = pe_data[atm][pe_data[atm]["datetime"] <= scan_end]
            if not ce_before.empty and not pe_before.empty:
                return (atm, atm, ce_before.iloc[-1]["close"], pe_before.iloc[-1]["close"], scan_end)

        return None

    def run_day(self, date: datetime) -> Optional[DayResult]:
        """Run backtest for a single day."""
        date_str = date.strftime("%Y-%m-%d")
        self.logger.info(f"Processing {date_str}...")

        # Load spot data
        spot_df = self.loader.load_spot_data(date_str)
        if spot_df is None or spot_df.empty:
            self.logger.debug(f"  No spot data for {date_str}")
            return None

        # Load VIX data
        vix_df = self.loader.load_vix_data(date_str)
        if vix_df is None or vix_df.empty:
            self.logger.debug(f"  No VIX data for {date_str}")
            return None

        # Get VIX open value (at 09:15)
        market_open = datetime.combine(date.date(), dtime(9, 15, 0))
        vix_open_row = vix_df[vix_df["datetime"] >= market_open]
        if vix_open_row.empty:
            return None
        vix_open = vix_open_row.iloc[0]["close"]

        # Day result
        day_result = DayResult(
            date=date,
            vix_open=vix_open,
            spot_open=spot_df.iloc[0]["close"],
            spot_close=spot_df.iloc[-1]["close"],
        )

        # VIX high/low for the day
        day_result.vix_high = vix_df["close"].max() if "close" in vix_df.columns else vix_open
        day_result.vix_low = vix_df["close"].min() if "close" in vix_df.columns else vix_open

        # Get lot size for this date
        lot_size = get_lot_size(date)

        # Get available strikes
        available_strikes = self.loader.get_available_strikes(date_str)
        if not available_strikes.get("CE") or not available_strikes.get("PE"):
            self.logger.debug(f"  No strikes available for {date_str}")
            return None

        # Track daily P&L for max loss check
        daily_pnl = 0.0
        trade_count = 0
        next_scan_start = datetime.combine(date.date(), self.config.get_scan_start())

        while trade_count < self.config.max_trades_per_day:
            # Check max daily loss
            max_loss = self.config.capital * (self.config.max_daily_loss_pct / 100)
            if daily_pnl < -max_loss:
                self.logger.info(f"  Max daily loss reached: {daily_pnl:.2f}")
                break

            # Check if we still have time for a trade
            exit_dt = datetime.combine(date.date(), self.config.get_exit_time())
            if next_scan_start >= exit_dt:
                break

            # Override scan times for re-entry
            original_scan_start = self.config.scan_start_time
            original_scan_end = self.config.scan_end_time

            if trade_count > 0:
                # For re-entry, scan from exit time for 2 minutes
                self.config.scan_start_time = next_scan_start.strftime("%H:%M:%S")
                scan_end_reentry = next_scan_start + timedelta(minutes=2)
                if scan_end_reentry.time() >= self.config.get_exit_time():
                    break
                self.config.scan_end_time = scan_end_reentry.strftime("%H:%M:%S")

            # Select strikes
            selection = self.select_strikes(date, spot_df, available_strikes)

            # Restore scan times
            self.config.scan_start_time = original_scan_start
            self.config.scan_end_time = original_scan_end

            if selection is None:
                self.logger.info(f"  Could not select strikes for trade #{trade_count + 1}")
                break

            ce_strike, pe_strike, ce_premium, pe_premium, entry_time = selection

            # Apply slippage to entry
            ce_entry = self.apply_slippage(ce_premium, is_buy=False)  # Selling
            pe_entry = self.apply_slippage(pe_premium, is_buy=False)

            total_premium = ce_entry + pe_entry
            hedge_distance = total_premium * self.config.hedge_multiplier

            # Round hedge distance to nearest strike interval
            hedge_distance = round(hedge_distance / self.config.nifty_strike_interval) * self.config.nifty_strike_interval
            if hedge_distance < self.config.nifty_strike_interval:
                hedge_distance = self.config.nifty_strike_interval

            # Hedge strikes (offset from sold strikes)
            hedge_ce_strike = ce_strike + hedge_distance
            hedge_pe_strike = pe_strike - hedge_distance

            # Load hedge data
            hedge_ce_df = self.loader.load_option_data(date_str, hedge_ce_strike, "CE")
            hedge_pe_df = self.loader.load_option_data(date_str, hedge_pe_strike, "PE")

            # Get hedge entry prices
            hedge_ce_price = 0.0
            hedge_pe_price = 0.0

            if hedge_ce_df is not None and not hedge_ce_df.empty:
                hce = hedge_ce_df[hedge_ce_df["datetime"] <= entry_time + timedelta(seconds=10)]
                if not hce.empty:
                    hedge_ce_price = self.apply_slippage(hce.iloc[-1]["close"], is_buy=True)
                else:
                    hce_after = hedge_ce_df[hedge_ce_df["datetime"] >= entry_time]
                    if not hce_after.empty:
                        hedge_ce_price = self.apply_slippage(hce_after.iloc[0]["close"], is_buy=True)

            if hedge_pe_df is not None and not hedge_pe_df.empty:
                hpe = hedge_pe_df[hedge_pe_df["datetime"] <= entry_time + timedelta(seconds=10)]
                if not hpe.empty:
                    hedge_pe_price = self.apply_slippage(hpe.iloc[-1]["close"], is_buy=True)
                else:
                    hpe_after = hedge_pe_df[hedge_pe_df["datetime"] >= entry_time]
                    if not hpe_after.empty:
                        hedge_pe_price = self.apply_slippage(hpe_after.iloc[0]["close"], is_buy=True)

            # Determine SL based on VIX
            sl_pct = self.get_sl_pct_for_vix(vix_open)

            # Create trade group
            self.trade_counter += 1
            trade = TradeGroup(
                trade_id=self.trade_counter,
                date=date,
                atm_strike=self.find_atm_strike(spot_df[spot_df["datetime"] <= entry_time].iloc[-1]["close"]),
                vix_at_entry=vix_open,
                sl_pct=sl_pct,
                lot_size=lot_size,
                num_lots=self.config.num_lots,
            )

            # Add legs
            trade.legs["SOLD_CE"] = TradeLeg(
                leg_type="SOLD_CE", strike=ce_strike, option_type="Call",
                entry_price=ce_entry, entry_time=entry_time,
                sl_price=self.round_sl_up(ce_entry * (1 + sl_pct / 100)),
            )
            trade.legs["SOLD_PE"] = TradeLeg(
                leg_type="SOLD_PE", strike=pe_strike, option_type="Put",
                entry_price=pe_entry, entry_time=entry_time,
                sl_price=self.round_sl_up(pe_entry * (1 + sl_pct / 100)),
            )
            trade.legs["HEDGE_CE"] = TradeLeg(
                leg_type="HEDGE_CE", strike=hedge_ce_strike, option_type="Call",
                entry_price=hedge_ce_price, entry_time=entry_time,
            )
            trade.legs["HEDGE_PE"] = TradeLeg(
                leg_type="HEDGE_PE", strike=hedge_pe_strike, option_type="Put",
                entry_price=hedge_pe_price, entry_time=entry_time,
            )
            trade.total_premium_received = total_premium

            self.logger.info(
                f"  Trade #{trade.trade_id}: SELL CE {ce_strike}@{ce_entry:.2f} + "
                f"PE {pe_strike}@{pe_entry:.2f} | "
                f"HEDGE CE {hedge_ce_strike}@{hedge_ce_price:.2f} + "
                f"PE {hedge_pe_strike}@{hedge_pe_price:.2f} | "
                f"SL%={sl_pct:.1f} VIX={vix_open:.2f}"
            )

            # ── Run the trade tick by tick ──
            trade_pnl = self._run_trade(
                trade, date, spot_df, vix_df,
                self.loader.load_option_data(date_str, ce_strike, "CE"),
                self.loader.load_option_data(date_str, pe_strike, "PE"),
                hedge_ce_df, hedge_pe_df,
            )

            trade.total_pnl = trade_pnl
            daily_pnl += trade_pnl
            day_result.trades.append(trade)
            self.trade_log.append(trade)
            trade_count += 1

            # Determine next scan start for re-entry
            last_exit_time = max(
                (leg.exit_time for leg in trade.legs.values() if leg.exit_time is not None),
                default=exit_dt,
            )
            next_scan_start = last_exit_time + timedelta(minutes=1)

        day_result.daily_pnl = daily_pnl
        return day_result

    def _run_trade(
        self, trade: TradeGroup, date: datetime,
        spot_df: pd.DataFrame, vix_df: pd.DataFrame,
        sold_ce_df: Optional[pd.DataFrame], sold_pe_df: Optional[pd.DataFrame],
        hedge_ce_df: Optional[pd.DataFrame], hedge_pe_df: Optional[pd.DataFrame],
    ) -> float:
        """
        Run a single trade from entry to exit, processing tick by tick.
        Returns total P&L in points (per lot).
        """
        exit_dt = datetime.combine(date.date(), self.config.get_exit_time())
        entry_time = trade.legs["SOLD_CE"].entry_time
        lot_size = trade.lot_size
        num_lots = trade.num_lots
        qty = lot_size * num_lots  # Total quantity

        # VIX spike tracking
        vix_open = trade.vix_at_entry
        vix_spike_triggered = False
        sl_pct = trade.sl_pct

        # ATR calculators (initialized when needed)
        atr_sold: Optional[ATRCalculator] = None
        atr_hedge: Optional[ATRCalculator] = None
        trailing_mode = False
        first_sl_leg = None  # "SOLD_CE" or "SOLD_PE"

        # Collect all timestamps we need to check
        # Build a unified timeline from all option data
        all_times = set()

        for df in [sold_ce_df, sold_pe_df, hedge_ce_df, hedge_pe_df]:
            if df is not None and not df.empty:
                times = df[df["datetime"] > entry_time]["datetime"].tolist()
                all_times.update(times)

        # Also add VIX timestamps
        if vix_df is not None:
            vix_times = vix_df[vix_df["datetime"] > entry_time]["datetime"].tolist()
            all_times.update(vix_times)

        all_times = sorted([t for t in all_times if t <= exit_dt])

        if not all_times:
            # No data after entry — exit at entry (flat)
            trade.exit_reason = "NO_DATA_AFTER_ENTRY"
            for leg in trade.legs.values():
                leg.exit_price = leg.entry_price
                leg.exit_time = entry_time
                leg.status = LegStatus.TARGET_EXIT
            return 0.0

        # ── Pre-build numpy arrays for O(log n) price lookups ──
        def build_lookup(df):
            """Convert DataFrame to sorted numpy arrays for fast searchsorted."""
            if df is None or df.empty:
                return None, None
            times = df["datetime"].values  # numpy datetime64 array, already sorted
            prices = df["close"].values.astype(np.float64)
            return times, prices

        def get_price_fast(times_arr, prices_arr, ts):
            """Get latest price at or before ts using searchsorted. O(log n)."""
            if times_arr is None:
                return None
            ts_np = np.datetime64(ts)
            idx = np.searchsorted(times_arr, ts_np, side="right") - 1
            if idx < 0:
                return None
            return float(prices_arr[idx])

        # Build lookups once
        sold_ce_t, sold_ce_p = build_lookup(sold_ce_df)
        sold_pe_t, sold_pe_p = build_lookup(sold_pe_df)
        hedge_ce_t, hedge_ce_p = build_lookup(hedge_ce_df)
        hedge_pe_t, hedge_pe_p = build_lookup(hedge_pe_df)
        vix_t, vix_p = build_lookup(vix_df)

        # Map leg keys to their lookup arrays
        sold_lookup = {
            "SOLD_CE": (sold_ce_t, sold_ce_p),
            "SOLD_PE": (sold_pe_t, sold_pe_p),
        }
        hedge_lookup = {
            "HEDGE_CE": (hedge_ce_t, hedge_ce_p),
            "HEDGE_PE": (hedge_pe_t, hedge_pe_p),
        }

        # Process timeline
        for ts in all_times:
            # ── Check VIX spike ──
            if self.config.vix_spike_enabled and not vix_spike_triggered:
                current_vix = get_price_fast(vix_t, vix_p, ts)
                if current_vix is not None:
                    vix_change_pct = ((current_vix - vix_open) / vix_open) * 100
                    if vix_change_pct >= self.config.vix_spike_threshold_pct:
                        vix_spike_triggered = True
                        new_sl_pct = self.config.vix_spike_new_sl_pct
                        self.logger.info(
                            f"    VIX SPIKE at {ts.strftime('%H:%M:%S')}: "
                            f"{vix_open:.2f}→{current_vix:.2f} ({vix_change_pct:+.1f}%) "
                            f"| SL changed: {sl_pct:.1f}%→{new_sl_pct:.1f}%"
                        )
                        sl_pct = new_sl_pct
                        trade.sl_pct = sl_pct

                        # Update SL prices for active sold legs
                        for key in ["SOLD_CE", "SOLD_PE"]:
                            leg = trade.legs[key]
                            if leg.status == LegStatus.ACTIVE:
                                leg.sl_price = self.round_sl_up(leg.entry_price * (1 + sl_pct / 100))

            # ── If not in trailing mode: check SL for sold legs ──
            if not trailing_mode:
                for key in ["SOLD_CE", "SOLD_PE"]:
                    leg = trade.legs[key]
                    if leg.status != LegStatus.ACTIVE:
                        continue

                    opt_type_str = "CE" if leg.option_type == "Call" else "PE"
                    t_arr, p_arr = sold_lookup[key]
                    price = get_price_fast(t_arr, p_arr, ts)

                    if price is not None and leg.sl_price is not None:
                        if price >= leg.sl_price:
                            # SL HIT
                            exit_price = self.apply_slippage(price, is_buy=True)  # Buying back
                            leg.exit_price = exit_price
                            leg.exit_time = ts
                            leg.status = LegStatus.SL_HIT
                            leg.pnl = (leg.entry_price - exit_price) * qty

                            self.logger.info(
                                f"    SL HIT: {key} {leg.strike} @ {exit_price:.2f} "
                                f"(entry={leg.entry_price:.2f}, SL={leg.sl_price:.2f})"
                            )

                            # Enter trailing mode
                            first_sl_leg = key
                            trade.first_sl_leg = key
                            trailing_mode = True

                            # Initialize ATR calculators
                            surviving_key = "SOLD_PE" if key == "SOLD_CE" else "SOLD_CE"
                            stopped_hedge_key = "HEDGE_CE" if key == "SOLD_CE" else "HEDGE_PE"

                            atr_sold = ATRCalculator(
                                self.config.atr_sold_timeframe_minutes,
                                self.config.atr_sold_period,
                                self.config.atr_sold_multiplier,
                            )
                            atr_hedge = ATRCalculator(
                                self.config.atr_hedge_timeframe_minutes,
                                self.config.atr_hedge_period,
                                self.config.atr_hedge_multiplier,
                            )

                            # Feed historical data to ATR calculators
                            surviving_df = sold_pe_df if key == "SOLD_CE" else sold_ce_df
                            hedge_df_trail = hedge_ce_df if key == "SOLD_CE" else hedge_pe_df

                            # Feed past ticks to build ATR history
                            if surviving_df is not None:
                                past = surviving_df[
                                    (surviving_df["datetime"] >= entry_time) &
                                    (surviving_df["datetime"] <= ts)
                                ]
                                for _, row in past.iterrows():
                                    atr_sold.update(row["datetime"], row["close"])

                            if hedge_df_trail is not None:
                                past_h = hedge_df_trail[
                                    (hedge_df_trail["datetime"] >= entry_time) &
                                    (hedge_df_trail["datetime"] <= ts)
                                ]
                                for _, row in past_h.iterrows():
                                    atr_hedge.update(row["datetime"], row["close"])

                            # Mark surviving sold leg as TRAILING
                            trade.legs[surviving_key].status = LegStatus.TRAILING
                            break  # Process one SL at a time

            # ── If in trailing mode: update ATR and check trailing SL ──
            if trailing_mode:
                surviving_key = "SOLD_PE" if first_sl_leg == "SOLD_CE" else "SOLD_CE"
                stopped_hedge_key = "HEDGE_CE" if first_sl_leg == "SOLD_CE" else "HEDGE_PE"

                surviving_leg = trade.legs[surviving_key]
                hedge_leg = trade.legs[stopped_hedge_key]

                # Update ATR for surviving sold leg
                if surviving_leg.status == LegStatus.TRAILING:
                    s_t, s_p = sold_lookup[surviving_key]
                    price = get_price_fast(s_t, s_p, ts)
                    if price is not None and atr_sold is not None:
                        atr_sold.update(ts, price)
                        trail_sl = atr_sold.get_trailing_sl_for_short()
                        if trail_sl is not None:
                            surviving_leg.trailing_sl = trail_sl

                            if price >= trail_sl:
                                # Trailing SL hit on surviving sold leg
                                exit_price = self.apply_slippage(price, is_buy=True)
                                surviving_leg.exit_price = exit_price
                                surviving_leg.exit_time = ts
                                surviving_leg.status = LegStatus.TRAIL_SL_HIT
                                surviving_leg.pnl = (surviving_leg.entry_price - exit_price) * qty

                                self.logger.info(
                                    f"    TRAIL SL HIT: {surviving_key} {surviving_leg.strike} "
                                    f"@ {exit_price:.2f} (trail={trail_sl:.2f})"
                                )

                                # Exit all remaining legs
                                all_lookups = {**sold_lookup, **hedge_lookup}
                                self._exit_all_remaining(trade, ts, all_lookups, qty)
                                trade.exit_reason = "BOTH_SL_HIT"
                                return self._calc_trade_pnl(trade, qty)

                # Update ATR for stopped leg's hedge
                if hedge_leg.status == LegStatus.ACTIVE:
                    h_t, h_p = hedge_lookup[stopped_hedge_key]
                    h_price = get_price_fast(h_t, h_p, ts)
                    if h_price is not None and atr_hedge is not None:
                        atr_hedge.update(ts, h_price)
                        trail_sl = atr_hedge.get_trailing_sl_for_long()
                        if trail_sl is not None:
                            hedge_leg.trailing_sl = trail_sl

                            if h_price <= trail_sl:
                                # Trailing SL hit on hedge
                                exit_price = self.apply_slippage(h_price, is_buy=False)  # Selling hedge
                                hedge_leg.exit_price = exit_price
                                hedge_leg.exit_time = ts
                                hedge_leg.status = LegStatus.TRAIL_SL_HIT
                                hedge_leg.pnl = (exit_price - hedge_leg.entry_price) * qty

                                self.logger.info(
                                    f"    HEDGE TRAIL SL: {stopped_hedge_key} "
                                    f"{hedge_leg.strike} @ {exit_price:.2f}"
                                )

        # ── Time exit: square off everything at exit_time ──
        all_lookups = {**sold_lookup, **hedge_lookup}
        self._exit_all_remaining(trade, exit_dt, all_lookups, qty)
        if not trade.exit_reason:
            trade.exit_reason = "TIME_EXIT"

        return self._calc_trade_pnl(trade, qty)

    def _exit_all_remaining(
        self, trade: TradeGroup, ts: datetime,
        lookup_map: Dict[str, Tuple], qty: int
    ):
        """Exit all legs that are still active/trailing.
        lookup_map: dict of leg_key -> (times_arr, prices_arr) numpy arrays.
        """
        for key, leg in trade.legs.items():
            if leg.exit_price is not None:
                continue  # Already exited

            price = None
            if key in lookup_map:
                t_arr, p_arr = lookup_map[key]
                if t_arr is not None:
                    ts_np = np.datetime64(ts)
                    idx = np.searchsorted(t_arr, ts_np, side="right") - 1
                    if idx >= 0:
                        price = float(p_arr[idx])

            if price is None:
                price = leg.entry_price  # Fallback

            if leg.is_sold:
                exit_price = self.apply_slippage(price, is_buy=True)
                leg.pnl = (leg.entry_price - exit_price) * qty
            else:
                exit_price = self.apply_slippage(price, is_buy=False)
                leg.pnl = (exit_price - leg.entry_price) * qty

            leg.exit_price = exit_price
            leg.exit_time = ts
            if leg.status in (LegStatus.ACTIVE, LegStatus.TRAILING):
                leg.status = LegStatus.TARGET_EXIT

    def _calc_trade_pnl(self, trade: TradeGroup, qty: int) -> float:
        """Calculate total P&L for a trade in INR."""
        total = sum(leg.pnl for leg in trade.legs.values())
        return total


# ─── PARALLEL WORKER (module-level for pickling) ────────────────────────────────

def _process_single_day(args):
    """
    Worker function for multiprocessing. Runs one day's backtest.
    Must be top-level (not a method) for ProcessPoolExecutor to pickle it.
    """
    date_str, config_dict, vix_ranges_list = args

    # Reconstruct config from dict
    config = BacktestConfig()
    for key, val in config_dict.items():
        if key == "vix_ranges":
            continue
        setattr(config, key, val)
    config.vix_ranges = [VIXRange(vr["vix_low"], vr["vix_high"], vr["sl_pct"]) for vr in vix_ranges_list]

    # Suppress logging in worker processes
    config.log_level = "WARNING"

    bt = StraddleBacktester(config)
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    result = bt.run_day(dt)

    if result is None:
        return None

    # Return serializable data (not the full objects with loggers)
    trades_data = []
    for t in result.trades:
        legs_data = {}
        for k, leg in t.legs.items():
            legs_data[k] = {
                "leg_type": leg.leg_type, "strike": leg.strike,
                "option_type": leg.option_type, "entry_price": leg.entry_price,
                "entry_time": leg.entry_time, "exit_price": leg.exit_price,
                "exit_time": leg.exit_time, "status": leg.status.value,
                "sl_price": leg.sl_price, "trailing_sl": leg.trailing_sl,
                "pnl": leg.pnl,
            }
        trades_data.append({
            "trade_id": t.trade_id, "date": t.date,
            "atm_strike": t.atm_strike, "vix_at_entry": t.vix_at_entry,
            "sl_pct": t.sl_pct, "total_premium": t.total_premium_received,
            "lot_size": t.lot_size, "num_lots": t.num_lots,
            "total_pnl": t.total_pnl, "exit_reason": t.exit_reason,
            "first_sl_leg": t.first_sl_leg, "legs": legs_data,
        })

    return {
        "date": result.date, "daily_pnl": result.daily_pnl,
        "vix_open": result.vix_open, "vix_high": result.vix_high,
        "vix_low": result.vix_low, "spot_open": result.spot_open,
        "spot_close": result.spot_close, "trades": trades_data,
    }


# ─── BACKTEST RUN & REPORT ──────────────────────────────────────────────────────
# These are defined at module level and attached to StraddleBacktester below,
# because _process_single_day (the parallel worker) must also be at module level.

def _bt_run(self):
    """Run the full backtest — parallel across CPU cores."""
    import multiprocessing
    max_workers = max(1, multiprocessing.cpu_count() - 1)

    console.print(Panel(
        "[bold yellow]BALFUND NIFTY STRADDLE BACKTESTER[/bold yellow]\n"
        f"[dim]Capital: ₹{self.config.capital:,.0f} | "
        f"Lots: {self.config.num_lots} | "
        f"Slippage: {self.config.slippage_pct}% | "
        f"Workers: {max_workers} cores[/dim]",
        box=box.DOUBLE,
        style="bold blue",
    ))

    # Get all available dates
    all_dates = self.loader.get_available_dates()
    if not all_dates:
        console.print("[red]No data found! Check your data directories.[/red]")
        return

    # Filter by date range
    start = datetime.strptime(self.config.start_date, "%Y-%m-%d")
    end = datetime.strptime(self.config.end_date, "%Y-%m-%d")

    dates = [d for d in all_dates
             if start <= datetime.strptime(d, "%Y-%m-%d") <= end]

    if not dates:
        console.print(f"[red]No dates in range {self.config.start_date} to {self.config.end_date}[/red]")
        return

    console.print(f"\n📅 Date range: {dates[0]} → {dates[-1]} ({len(dates)} trading days)")
    console.print(f"📊 VIX ranges: {self.config.vix_ranges}")
    console.print(f"🎯 Strike scan: {self.config.scan_start_time} → {self.config.scan_end_time}")
    console.print(f"⏰ Exit time: {self.config.exit_time}")
    if self.config.vix_spike_enabled:
        console.print(
            f"⚡ VIX spike: +{self.config.vix_spike_threshold_pct}% → "
            f"SL={self.config.vix_spike_new_sl_pct}%"
        )
    console.print()

    # ── Phase 1: Pre-convert all CSVs to Parquet ──
    if self.config.convert_to_parquet:
        self.loader.preconvert_all_csvs(dates)

    # ── Phase 2: Backtest (parallel) ──
    config_dict = {k: v for k, v in self.config.__dict__.items() if k != "vix_ranges"}
    vix_ranges_list = [
        {"vix_low": vr.vix_low, "vix_high": vr.vix_high, "sl_pct": vr.sl_pct}
        for vr in self.config.vix_ranges
    ]
    worker_args = [(d, config_dict, vix_ranges_list) for d in dates]

    console.print(f"[cyan]⚡ Running backtest on {max_workers} CPU cores...[/cyan]")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=40),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("•"),
        TextColumn("[cyan]{task.completed}/{task.total} days"),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Backtesting", total=len(dates))

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_process_single_day, arg): arg[0] for arg in worker_args}

            for future in as_completed(futures):
                date_str = futures[future]
                progress.update(task, description=f"Completed {date_str}")
                progress.advance(task)

                try:
                    result_data = future.result()
                except Exception as e:
                    self.logger.error(f"  Error on {date_str}: {e}")
                    continue

                if result_data is None:
                    continue

                day_result = DayResult(
                    date=result_data["date"],
                    daily_pnl=result_data["daily_pnl"],
                    vix_open=result_data["vix_open"],
                    vix_high=result_data["vix_high"],
                    vix_low=result_data["vix_low"],
                    spot_open=result_data["spot_open"],
                    spot_close=result_data["spot_close"],
                )

                for td in result_data["trades"]:
                    trade = TradeGroup(
                        trade_id=td["trade_id"], date=td["date"],
                        atm_strike=td["atm_strike"], vix_at_entry=td["vix_at_entry"],
                        sl_pct=td["sl_pct"], total_premium_received=td["total_premium"],
                        lot_size=td["lot_size"], num_lots=td["num_lots"],
                        total_pnl=td["total_pnl"], exit_reason=td["exit_reason"],
                        first_sl_leg=td["first_sl_leg"],
                    )
                    for k, ld in td["legs"].items():
                        trade.legs[k] = TradeLeg(
                            leg_type=ld["leg_type"], strike=ld["strike"],
                            option_type=ld["option_type"],
                            entry_price=ld["entry_price"], entry_time=ld["entry_time"],
                            exit_price=ld["exit_price"], exit_time=ld["exit_time"],
                            status=LegStatus(ld["status"]),
                            sl_price=ld["sl_price"], trailing_sl=ld["trailing_sl"],
                            pnl=ld["pnl"],
                        )
                    day_result.trades.append(trade)
                    self.trade_log.append(trade)

                self.results.append(day_result)

    # Sort results by date (futures complete out of order)
    self.results.sort(key=lambda r: r.date)
    self.trade_log.sort(key=lambda t: t.date)

    # Re-number trades sequentially
    for i, t in enumerate(self.trade_log, 1):
        t.trade_id = i

    # Generate report
    self._generate_report()


def _bt_generate_report(self):
    """Generate and display backtest results."""
    if not self.results:
        console.print("[red]No results to report.[/red]")
        return

    console.print("\n")
    console.print(Panel(
        "[bold green]═══ BACKTEST RESULTS ═══[/bold green]",
        box=box.HEAVY,
    ))

    total_pnl = sum(r.daily_pnl for r in self.results)
    trading_days = len(self.results)
    total_trades = len(self.trade_log)
    winning_days = sum(1 for r in self.results if r.daily_pnl > 0)
    losing_days = sum(1 for r in self.results if r.daily_pnl < 0)

    winning_trades = sum(1 for t in self.trade_log if t.total_pnl > 0)
    losing_trades = sum(1 for t in self.trade_log if t.total_pnl < 0)

    cumulative = []
    running = 0
    peak = 0
    max_dd = 0
    for r in self.results:
        running += r.daily_pnl
        cumulative.append(running)
        peak = max(peak, running)
        dd = peak - running
        max_dd = max(max_dd, dd)

    avg_win = (
        np.mean([t.total_pnl for t in self.trade_log if t.total_pnl > 0])
        if winning_trades > 0 else 0
    )
    avg_loss = (
        np.mean([t.total_pnl for t in self.trade_log if t.total_pnl < 0])
        if losing_trades > 0 else 0
    )

    table = Table(title="📊 Performance Summary", box=box.ROUNDED, show_lines=True)
    table.add_column("Metric", style="cyan", justify="left")
    table.add_column("Value", style="white", justify="right")

    table.add_row("Total P&L", f"₹{total_pnl:,.2f}")
    table.add_row("Return on Capital", f"{(total_pnl / self.config.capital * 100):.2f}%")
    table.add_row("Trading Days", str(trading_days))
    table.add_row("Total Trades", str(total_trades))
    table.add_row("Win Rate (trades)", f"{(winning_trades / total_trades * 100):.1f}%" if total_trades > 0 else "N/A")
    table.add_row("Winning / Losing Days", f"{winning_days} / {losing_days}")
    table.add_row("Avg Win (per trade)", f"₹{avg_win:,.2f}")
    table.add_row("Avg Loss (per trade)", f"₹{avg_loss:,.2f}")
    table.add_row("Avg Win / Avg Loss", f"{abs(avg_win / avg_loss):.2f}" if avg_loss != 0 else "N/A")
    table.add_row("Max Drawdown", f"₹{max_dd:,.2f}")
    table.add_row("Profit Factor",
                   f"{abs(sum(t.total_pnl for t in self.trade_log if t.total_pnl > 0) / sum(t.total_pnl for t in self.trade_log if t.total_pnl < 0)):.2f}"
                   if losing_trades > 0 else "∞")

    console.print(table)

    exit_reasons = {}
    for t in self.trade_log:
        exit_reasons[t.exit_reason] = exit_reasons.get(t.exit_reason, 0) + 1

    reason_table = Table(title="🚪 Exit Reasons", box=box.SIMPLE)
    reason_table.add_column("Reason", style="yellow")
    reason_table.add_column("Count", style="white", justify="right")
    reason_table.add_column("Pct", style="dim", justify="right")
    for reason, count in sorted(exit_reasons.items(), key=lambda x: -x[1]):
        reason_table.add_row(reason, str(count), f"{count/total_trades*100:.1f}%")
    console.print(reason_table)

    self._save_results()


def _bt_save_results(self):
    """Save results to CSV files."""
    os.makedirs(self.config.output_dir, exist_ok=True)

    daily_data = []
    for r in self.results:
        daily_data.append({
            "date": r.date.strftime("%Y-%m-%d"),
            "daily_pnl": r.daily_pnl,
            "num_trades": len(r.trades),
            "vix_open": r.vix_open,
            "spot_open": r.spot_open,
            "spot_close": r.spot_close,
        })

    daily_df = pd.DataFrame(daily_data)
    daily_df.to_csv(os.path.join(self.config.output_dir, "daily_pnl.csv"), index=False)

    trade_data = []
    for t in self.trade_log:
        row = {
            "trade_id": t.trade_id,
            "date": t.date.strftime("%Y-%m-%d"),
            "entry_time": t.legs["SOLD_CE"].entry_time.strftime("%H:%M:%S") if t.legs.get("SOLD_CE") and t.legs["SOLD_CE"].entry_time else "",
            "atm_strike": t.atm_strike,
            "vix_at_entry": t.vix_at_entry,
            "sl_pct": t.sl_pct,
            "total_premium": t.total_premium_received,
            "lot_size": t.lot_size,
            "num_lots": t.num_lots,
            "total_pnl": t.total_pnl,
            "exit_reason": t.exit_reason,
        }
        for key, leg in t.legs.items():
            row[f"{key}_strike"] = leg.strike
            row[f"{key}_entry"] = leg.entry_price
            row[f"{key}_exit"] = leg.exit_price
            row[f"{key}_pnl"] = leg.pnl
            row[f"{key}_status"] = leg.status.value
        trade_data.append(row)

    trade_df = pd.DataFrame(trade_data)
    trade_df.to_csv(os.path.join(self.config.output_dir, "trade_log.csv"), index=False)

    if daily_data:
        daily_df["cumulative_pnl"] = daily_df["daily_pnl"].cumsum()
        daily_df.to_csv(os.path.join(self.config.output_dir, "cumulative_pnl.csv"), index=False)

    console.print(f"\n💾 Results saved to: [bold]{self.config.output_dir}/[/bold]")
    console.print(f"   ├── daily_pnl.csv")
    console.print(f"   ├── trade_log.csv")
    console.print(f"   └── cumulative_pnl.csv")


# Attach methods to StraddleBacktester
StraddleBacktester.run = _bt_run
StraddleBacktester._generate_report = _bt_generate_report
StraddleBacktester._save_results = _bt_save_results


# ─── CONFIG LOADER ───────────────────────────────────────────────────────────────

def load_config_from_json(path: str) -> BacktestConfig:
    """Load config from a JSON file."""
    with open(path) as f:
        data = json.load(f)

    config = BacktestConfig()

    # Simple fields
    for key in [
        "spot_data_dir", "vix_data_dir", "options_data_dir", "output_dir",
        "start_date", "end_date", "scan_start_time", "scan_end_time",
        "nifty_strike_interval", "hedge_multiplier",
        "vix_spike_enabled", "vix_spike_threshold_pct", "vix_spike_new_sl_pct",
        "exit_time", "atr_sold_timeframe_minutes", "atr_sold_period",
        "atr_sold_multiplier", "atr_hedge_timeframe_minutes", "atr_hedge_period",
        "atr_hedge_multiplier", "max_trades_per_day", "max_daily_loss_pct",
        "capital", "num_lots", "slippage_pct", "convert_to_parquet", "log_level",
    ]:
        if key in data:
            setattr(config, key, data[key])

    # VIX ranges
    if "vix_ranges" in data:
        config.vix_ranges = [
            VIXRange(vr["vix_low"], vr["vix_high"], vr["sl_pct"])
            for vr in data["vix_ranges"]
        ]

    return config


def save_default_config(path: str):
    """Save a default config JSON for the user to edit."""
    config = BacktestConfig()
    data = {
        "spot_data_dir": config.spot_data_dir,
        "vix_data_dir": config.vix_data_dir,
        "options_data_dir": config.options_data_dir,
        "output_dir": config.output_dir,
        "start_date": config.start_date,
        "end_date": config.end_date,
        "scan_start_time": config.scan_start_time,
        "scan_end_time": config.scan_end_time,
        "nifty_strike_interval": config.nifty_strike_interval,
        "hedge_multiplier": config.hedge_multiplier,
        "vix_ranges": [
            {"vix_low": vr.vix_low, "vix_high": vr.vix_high, "sl_pct": vr.sl_pct}
            for vr in config.vix_ranges
        ],
        "vix_spike_enabled": config.vix_spike_enabled,
        "vix_spike_threshold_pct": config.vix_spike_threshold_pct,
        "vix_spike_new_sl_pct": config.vix_spike_new_sl_pct,
        "exit_time": config.exit_time,
        "atr_sold_timeframe_minutes": config.atr_sold_timeframe_minutes,
        "atr_sold_period": config.atr_sold_period,
        "atr_sold_multiplier": config.atr_sold_multiplier,
        "atr_hedge_timeframe_minutes": config.atr_hedge_timeframe_minutes,
        "atr_hedge_period": config.atr_hedge_period,
        "atr_hedge_multiplier": config.atr_hedge_multiplier,
        "max_trades_per_day": config.max_trades_per_day,
        "max_daily_loss_pct": config.max_daily_loss_pct,
        "capital": config.capital,
        "num_lots": config.num_lots,
        "slippage_pct": config.slippage_pct,
        "convert_to_parquet": config.convert_to_parquet,
        "log_level": config.log_level,
    }

    with open(path, "w") as f:
        json.dump(data, f, indent=2)

    console.print(f"[green]Default config saved to: {path}[/green]")


# ─── CLI ENTRY POINT ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Balfund NIFTY Options Straddle Backtester",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate default config:
  python backtest_engine.py --generate-config

  # Run with config file:
  python backtest_engine.py --config config.json

  # Run with CLI overrides:
  python backtest_engine.py --config config.json --capital 2000000 --num-lots 2

  # Quick run with directories:
  python backtest_engine.py \\
    --spot-dir ./data/NIFTY_SPOT_1SEC \\
    --vix-dir ./data/INDVIX_1SEC \\
    --options-dir ./data/NIFTY_OPTIONS_1SEC \\
    --start-date 2023-01-01 --end-date 2024-12-31
        """,
    )

    parser.add_argument("--config", type=str, help="Path to config JSON file")
    parser.add_argument("--generate-config", action="store_true",
                        help="Generate default config.json and exit")

    # Directory overrides
    parser.add_argument("--spot-dir", type=str, help="NIFTY spot data directory")
    parser.add_argument("--vix-dir", type=str, help="INDVIX data directory")
    parser.add_argument("--options-dir", type=str, help="Options data directory")
    parser.add_argument("--output-dir", type=str, default="results", help="Output directory")

    # Date range
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")

    # Trade params
    parser.add_argument("--capital", type=float, help="Starting capital (INR)")
    parser.add_argument("--num-lots", type=int, help="Number of lots")
    parser.add_argument("--slippage", type=float, help="Slippage percentage")
    parser.add_argument("--max-trades", type=int, help="Max trades per day")
    parser.add_argument("--max-daily-loss", type=float, help="Max daily loss %% of capital")
    parser.add_argument("--exit-time", type=str, help="Exit time (HH:MM:SS)")
    parser.add_argument("--hedge-multiplier", type=float, help="Hedge distance multiplier (total premium × this)")

    # VIX spike
    parser.add_argument("--vix-spike-off", action="store_true", help="Disable VIX spike feature")
    parser.add_argument("--vix-spike-pct", type=float, help="VIX spike threshold %%")
    parser.add_argument("--vix-spike-sl", type=float, help="New SL %% after VIX spike")

    # Misc
    parser.add_argument("--no-parquet", action="store_true", help="Disable Parquet conversion")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])

    args = parser.parse_args()

    # Generate config mode
    if args.generate_config:
        save_default_config("config.json")
        return

    # Load config — auto-detect config.json in same folder if not specified
    if args.config:
        config = load_config_from_json(args.config)
    else:
        # Auto-detect config.json next to the script/EXE
        # When running as PyInstaller EXE, __file__ is in a temp folder
        # Use sys.executable's directory instead
        if getattr(sys, 'frozen', False):
            script_dir = os.path.dirname(sys.executable)
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
        auto_config = os.path.join(script_dir, "config.json")
        if os.path.exists(auto_config):
            console.print(f"[dim]Auto-detected config: {auto_config}[/dim]")
            config = load_config_from_json(auto_config)
        else:
            config = BacktestConfig()

    # Apply CLI overrides
    if args.spot_dir:
        config.spot_data_dir = args.spot_dir
    if args.vix_dir:
        config.vix_data_dir = args.vix_dir
    if args.options_dir:
        config.options_data_dir = args.options_dir
    if args.output_dir:
        config.output_dir = args.output_dir
    if args.start_date:
        config.start_date = args.start_date
    if args.end_date:
        config.end_date = args.end_date
    if args.capital:
        config.capital = args.capital
    if args.num_lots:
        config.num_lots = args.num_lots
    if args.slippage:
        config.slippage_pct = args.slippage
    if args.max_trades:
        config.max_trades_per_day = args.max_trades
    if args.max_daily_loss:
        config.max_daily_loss_pct = args.max_daily_loss
    if args.exit_time:
        config.exit_time = args.exit_time
    if args.hedge_multiplier:
        config.hedge_multiplier = args.hedge_multiplier
    if args.vix_spike_off:
        config.vix_spike_enabled = False
    if args.vix_spike_pct:
        config.vix_spike_threshold_pct = args.vix_spike_pct
    if args.vix_spike_sl:
        config.vix_spike_new_sl_pct = args.vix_spike_sl
    if args.no_parquet:
        config.convert_to_parquet = False
    config.log_level = args.log_level

    # Validate required dirs
    if not config.spot_data_dir or not config.vix_data_dir or not config.options_data_dir:
        console.print("[red]Error: --spot-dir, --vix-dir, and --options-dir are required![/red]")
        console.print("[dim]Use --generate-config to create a config file template.[/dim]")
        sys.exit(1)

    for d in [config.spot_data_dir, config.vix_data_dir, config.options_data_dir]:
        if not os.path.isdir(d):
            console.print(f"[red]Directory not found: {d}[/red]")
            sys.exit(1)

    # Run backtest
    backtester = StraddleBacktester(config)
    start_time = time.time()
    backtester.run()
    elapsed = time.time() - start_time

    console.print(f"\n⏱  Backtest completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
