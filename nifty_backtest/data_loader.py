"""
data_loader.py — Reads local Breeze CSV files directly.

Folder structure expected:
    base_path/
        INDVIX_1SEC/
            2025-11-25.csv          ← VIX tick data
        NIFTY_SPOT_1SEC/
            2025-11-25.csv          ← NIFTY spot tick data
        NIFTY_OPTIONS_1SEC/
            2025-12-16/             ← subfolder = expiry date (YYYY-MM-DD)
                2025-12-15_24000_CE.csv
                2025-12-15_24000_PE.csv
                ...

All CSV files have columns:
    close, datetime, exchange_code, high, low, open, stock_code, volume
    (options also have: expiry_date, open_interest, product_type, right, strike_price)

Data is 1-second tick data → resampled to 1-minute OHLC internally.
"""

import os
import glob
import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# PATH CONFIG
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PathConfig:
    """Configure folder paths for local data."""
    base_path: str = r"C:\Users\Admin\Downloads\BreezeDownloader-v1.4.2\breeze_data"

    @property
    def vix_dir(self) -> Path:
        return Path(self.base_path) / "INDVIX_1SEC"

    @property
    def spot_dir(self) -> Path:
        return Path(self.base_path) / "NIFTY_SPOT_1SEC"

    @property
    def options_dir(self) -> Path:
        return Path(self.base_path) / "NIFTY_OPTIONS_1SEC"


# ─────────────────────────────────────────────────────────────────────────────
# DAY DATA CONTAINER
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class DayData:
    """All data needed to simulate a single trading day."""
    date_str:       str
    expiry_str:     str

    # 1-min resampled series (index = datetime)
    vix_1min:       pd.DataFrame = field(default_factory=pd.DataFrame)
    spot_1min:      pd.DataFrame = field(default_factory=pd.DataFrame)

    # options_1min[(strike, 'CE')] = DataFrame with 1-min OHLC
    options_1min:   Dict[Tuple[int, str], pd.DataFrame] = field(default_factory=dict)

    vix_prev_close: Optional[float] = None
    available_strikes: List[int]    = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return (
            not self.vix_1min.empty
            and not self.spot_1min.empty
            and len(self.options_1min) > 0
            and self.vix_prev_close is not None
        )


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADER
# ─────────────────────────────────────────────────────────────────────────────

class DataLoader:
    """
    Loads local Breeze CSV data and resamples to 1-minute OHLC.
    Caches loaded days in memory so grid search doesn't re-read files.
    """

    def __init__(self, path_config: PathConfig = None):
        self.paths = path_config or PathConfig()
        self._cache: Dict[str, DayData] = {}          # date_str → DayData
        self._vix_daily_cache: Dict[str, float] = {}  # date_str → prev_close

    # ─── Public API ──────────────────────────────────────────────────────────

    def load_day(self, date_str: str, expiry_str: str) -> DayData:
        """
        Load and resample all data for a trading day.
        Uses cache if already loaded.
        """
        cache_key = f"{date_str}_{expiry_str}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        logger.info(f"Loading data for {date_str} (expiry: {expiry_str})")
        day = DayData(date_str=date_str, expiry_str=expiry_str)

        # VIX
        day.vix_1min       = self._load_vix_1min(date_str)
        day.vix_prev_close = self._get_vix_prev_close(date_str)

        # NIFTY spot
        day.spot_1min = self._load_spot_1min(date_str)

        # Options
        day.options_1min, day.available_strikes = self._load_options(
            date_str, expiry_str
        )

        self._cache[cache_key] = day
        return day

    def get_available_trading_dates(
        self,
        from_date: str,
        to_date: str,
        expiry_map: Dict[str, str] = None,
    ) -> List[Tuple[str, str]]:
        """
        Scan NIFTY_SPOT_1SEC folder to find all available trading dates.
        Returns list of (date_str, expiry_str) tuples.
        If expiry_map not provided, auto-computes Thursday expiry.
        """
        spot_dir = self.paths.spot_dir
        if not spot_dir.exists():
            raise FileNotFoundError(f"Spot data folder not found: {spot_dir}")

        csv_files = sorted(spot_dir.glob("*.csv"))
        results   = []

        for f in csv_files:
            date_str = f.stem  # filename without .csv
            if not _is_valid_date(date_str):
                continue
            if not (from_date <= date_str <= to_date):
                continue

            expiry_str = (
                expiry_map.get(date_str)
                if expiry_map
                else _get_nearest_thursday(date_str)
            )
            results.append((date_str, expiry_str))

        logger.info(f"Found {len(results)} trading days between {from_date} and {to_date}")
        return results

    def preload_all(
        self,
        from_date: str,
        to_date: str,
        expiry_map: Dict[str, str] = None,
    ) -> List[Tuple[str, DayData]]:
        """
        Preload ALL days into memory before grid search starts.
        This avoids re-reading files for every parameter combination.
        Returns list of (date_str, DayData).
        """
        trading_days = self.get_available_trading_dates(from_date, to_date, expiry_map)
        loaded = []

        for i, (date_str, expiry_str) in enumerate(trading_days):
            logger.info(f"Preloading [{i+1}/{len(trading_days)}] {date_str}")
            day = self.load_day(date_str, expiry_str)
            if day.is_valid:
                loaded.append((date_str, day))
            else:
                missing = []
                if day.vix_1min.empty:         missing.append("VIX")
                if day.spot_1min.empty:        missing.append("Spot")
                if not day.options_1min:       missing.append("Options")
                if day.vix_prev_close is None: missing.append("VIX_prev_close")
                logger.warning(f"  Skipping {date_str} — missing: {', '.join(missing)}")

        logger.info(f"Preloaded {len(loaded)}/{len(trading_days)} valid days")
        return loaded

    # ─── VIX ─────────────────────────────────────────────────────────────────

    def _load_vix_1min(self, date_str: str) -> pd.DataFrame:
        path = self.paths.vix_dir / f"{date_str}.csv"
        if not path.exists():
            logger.warning(f"VIX file not found: {path}")
            return pd.DataFrame()
        raw = _read_csv(path)
        if raw.empty:
            return pd.DataFrame()
        return _resample_1min(raw, date_str)

    def _get_vix_prev_close(self, date_str: str) -> Optional[float]:
        """Get previous trading day's VIX last close value."""
        if date_str in self._vix_daily_cache:
            return self._vix_daily_cache[date_str]

        # Find previous CSV file in the VIX folder
        vix_dir = self.paths.vix_dir
        if not vix_dir.exists():
            return None

        all_dates = sorted([
            f.stem for f in vix_dir.glob("*.csv")
            if _is_valid_date(f.stem)
        ])

        prev_dates = [d for d in all_dates if d < date_str]
        if not prev_dates:
            logger.warning(f"No previous VIX date found before {date_str}")
            return None

        prev_date = prev_dates[-1]
        prev_path = vix_dir / f"{prev_date}.csv"
        raw = _read_csv(prev_path)
        if raw.empty:
            return None

        # Take the last close of the day (day close = last tick)
        last_close = float(raw["close"].iloc[-1])
        self._vix_daily_cache[date_str] = last_close
        logger.debug(f"VIX prev close for {date_str}: {last_close} (from {prev_date})")
        return last_close

    # ─── NIFTY Spot ──────────────────────────────────────────────────────────

    def _load_spot_1min(self, date_str: str) -> pd.DataFrame:
        path = self.paths.spot_dir / f"{date_str}.csv"
        if not path.exists():
            logger.warning(f"Spot file not found: {path}")
            return pd.DataFrame()
        raw = _read_csv(path)
        if raw.empty:
            return pd.DataFrame()
        return _resample_1min(raw, date_str)

    # ─── Options ─────────────────────────────────────────────────────────────

    def _load_options(
        self,
        date_str: str,
        expiry_str: str,
    ) -> Tuple[Dict[Tuple[int, str], pd.DataFrame], List[int]]:
        """
        Load all option CSVs for a given trade date and expiry.
        Returns (options_dict, strikes_list).

        Tries expiry folder in formats:
            YYYY-MM-DD  (e.g. 2025-12-16)
            DD-MON-YYYY (e.g. 16-DEC-2025)
        """
        options_dir = self.paths.options_dir

        # Try multiple folder name formats for expiry
        expiry_folder = _find_expiry_folder(options_dir, expiry_str)
        if expiry_folder is None:
            logger.warning(f"Expiry folder not found for {expiry_str} in {options_dir}")
            return {}, []

        # Pattern: {date_str}_{strike}_{CE/PE}.csv
        pattern = str(expiry_folder / f"{date_str}_*.csv")
        files   = glob.glob(pattern)

        if not files:
            logger.warning(f"No option files found: {pattern}")
            return {}, []

        options_dict = {}
        strikes_set  = set()

        for fpath in sorted(files):
            fname    = Path(fpath).stem        # e.g. 2025-12-15_24000_CE
            parts    = fname.split("_")
            if len(parts) < 3:
                continue

            opt_type = parts[-1].upper()       # CE or PE
            if opt_type not in ("CE", "PE"):
                continue

            try:
                strike = int(float(parts[-2]))  # e.g. 24000
            except ValueError:
                continue

            raw = _read_csv(fpath)
            if raw.empty:
                continue

            df_1min = _resample_1min(raw, date_str)
            if df_1min.empty:
                continue

            options_dict[(strike, opt_type)] = df_1min
            strikes_set.add(strike)

        strikes_list = sorted(strikes_set)
        logger.info(f"  Loaded {len(options_dict)} option series "
                    f"({len(strikes_list)} strikes) for {date_str}")
        return options_dict, strikes_list

    # ─── Stats ────────────────────────────────────────────────────────────────

    def stats(self, from_date: str = None, to_date: str = None):
        """Print summary of available local data."""
        print("\n=== Local Data Statistics ===")

        for label, folder in [
            ("VIX",        self.paths.vix_dir),
            ("NIFTY Spot", self.paths.spot_dir),
            ("Options",    self.paths.options_dir),
        ]:
            if not folder.exists():
                print(f"  {label:<15}: ❌ Folder not found: {folder}")
                continue

            if label == "Options":
                expiry_folders = [d for d in folder.iterdir() if d.is_dir()]
                total_files    = sum(len(list(d.glob("*.csv"))) for d in expiry_folders)
                print(f"  {label:<15}: {len(expiry_folders)} expiry folders, "
                      f"{total_files:,} CSV files")
            else:
                files = sorted(folder.glob("*.csv"))
                dates = [f.stem for f in files if _is_valid_date(f.stem)]
                if dates:
                    rng = f"{dates[0]} → {dates[-1]}"
                    if from_date and to_date:
                        in_range = [d for d in dates if from_date <= d <= to_date]
                        print(f"  {label:<15}: {len(dates)} files ({rng}), "
                              f"{len(in_range)} in requested range")
                    else:
                        print(f"  {label:<15}: {len(dates)} files ({rng})")
                else:
                    print(f"  {label:<15}: No dated CSV files found in {folder}")

        if self._cache:
            print(f"\n  Cache: {len(self._cache)} days loaded in memory")
        print()


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def _read_csv(path) -> pd.DataFrame:
    """Read a Breeze CSV file into a clean DataFrame."""
    try:
        df = pd.read_csv(path)
        df.columns = [c.strip().lower() for c in df.columns]

        if "datetime" not in df.columns:
            logger.warning(f"No datetime column in {path}")
            return pd.DataFrame()

        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
        df.dropna(subset=["datetime"], inplace=True)

        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df.sort_values("datetime", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    except Exception as e:
        logger.error(f"Failed to read {path}: {e}")
        return pd.DataFrame()


def _resample_1min(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    Resample tick/1-sec data to 1-minute OHLC.
    Filters to market hours: 09:15 – 15:30.
    Index will be the 1-min candle close timestamp.
    """
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df.set_index("datetime", inplace=True)

    # Filter to market hours only
    date_prefix = f"{date_str} "
    mkt_open    = pd.Timestamp(f"{date_str} 09:15:00")
    mkt_close   = pd.Timestamp(f"{date_str} 15:30:00")
    df = df[(df.index >= mkt_open) & (df.index <= mkt_close)]

    if df.empty:
        return pd.DataFrame()

    # Keep only OHLCV columns
    agg_dict = {}
    if "open"   in df.columns: agg_dict["open"]   = "first"
    if "high"   in df.columns: agg_dict["high"]   = "max"
    if "low"    in df.columns: agg_dict["low"]    = "min"
    if "close"  in df.columns: agg_dict["close"]  = "last"
    if "volume" in df.columns: agg_dict["volume"] = "sum"
    if "open_interest" in df.columns:
        agg_dict["open_interest"] = "last"

    resampled = (
        df.resample("1T", label="right", closed="right")
        .agg(agg_dict)
        .dropna(subset=["close"])
    )

    # Rename open_interest → oi for consistency
    if "open_interest" in resampled.columns:
        resampled.rename(columns={"open_interest": "oi"}, inplace=True)

    return resampled


def _find_expiry_folder(options_dir: Path, expiry_str: str) -> Optional[Path]:
    """
    Look for expiry subfolder matching expiry_str in multiple formats.
    expiry_str is always YYYY-MM-DD internally.
    Folder may be named: YYYY-MM-DD or DD-MON-YYYY
    """
    if not options_dir.exists():
        return None

    # Format 1: YYYY-MM-DD (e.g. 2025-12-16)
    candidate1 = options_dir / expiry_str
    if candidate1.exists():
        return candidate1

    # Format 2: DD-MON-YYYY (e.g. 16-DEC-2025)
    try:
        dt = pd.to_datetime(expiry_str)
        candidate2 = options_dir / dt.strftime("%d-%b-%Y").upper()
        if candidate2.exists():
            return candidate2

        # Format 3: D-MON-YYYY without leading zero
        candidate3 = options_dir / dt.strftime("%-d-%b-%Y").upper()
        if candidate3.exists():
            return candidate3
    except Exception:
        pass

    # Fallback: scan all subfolders and fuzzy-match
    for sub in options_dir.iterdir():
        if not sub.is_dir():
            continue
        try:
            folder_date = pd.to_datetime(sub.name, dayfirst=True)
            target_date = pd.to_datetime(expiry_str)
            if folder_date.date() == target_date.date():
                return sub
        except Exception:
            continue

    return None


def _is_valid_date(s: str) -> bool:
    """Check if string is a valid YYYY-MM-DD date."""
    try:
        pd.to_datetime(s, format="%Y-%m-%d")
        return True
    except Exception:
        return False


def _get_nearest_thursday(date_str: str) -> str:
    """Return the nearest Thursday on or after date_str."""
    d = date.fromisoformat(date_str)
    days_ahead = 3 - d.weekday()   # Thursday = 3
    if days_ahead < 0:
        days_ahead += 7
    return (d + timedelta(days=days_ahead)).isoformat()
