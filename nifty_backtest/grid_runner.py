"""
grid_runner.py — Grid search using pre-loaded in-memory DayData.

Data is loaded ONCE before the grid search starts.
All parameter combinations run against the cached data — no re-reading files.
"""

import itertools
import logging
from typing import List, Dict, Tuple
from datetime import datetime

import pandas as pd

from config import StrategyParams, GridConfig
from data_loader import DataLoader, DayData, PathConfig
from day_simulator import DaySimulator, DayResult
from metrics import compute_metrics, rank_param_sets

logger = logging.getLogger(__name__)


def generate_param_combinations(grid: GridConfig) -> List[StrategyParams]:
    keys_and_values = [
        ("atm_scan_start",         grid.atm_scan_starts),
        ("atm_scan_end",           grid.atm_scan_ends),
        ("max_premium_diff",       grid.max_premium_diffs),
        ("hedge_pct",              grid.hedge_pcts),
        ("vix_intraday_threshold", grid.vix_intraday_thresholds),
        ("atr_timeframe",          grid.atr_timeframes),
        ("atr_period",             grid.atr_periods),
        ("atr_multiplier",         grid.atr_multipliers),
        ("slippage_pct",           grid.slippage_pcts),
        ("hedge_trail_step",       grid.hedge_trail_steps),
        ("eod_exit_times",         grid.eod_exit_times),
    ]
    keys   = [k for k, _ in keys_and_values]
    values = [v for _, v in keys_and_values]

    combos = []
    for combo in itertools.product(*values):
        p = StrategyParams()
        for key, val in zip(keys, combo):
            setattr(p, key, val)
        combos.append(p)
    return combos


class GridRunner:
    def __init__(self, path_config: PathConfig, grid: GridConfig = None):
        self.paths = path_config
        self.grid  = grid or GridConfig()

    def run(
        self,
        from_date: str,
        to_date: str,
        params_override: StrategyParams = None,
    ) -> "GridRunResult":
        # Step 1: Load all data into memory once
        loader = DataLoader(self.paths)
        logger.info("Preloading all trading day data...")
        loaded_days: List[Tuple[str, DayData]] = loader.preload_all(from_date, to_date)

        if not loaded_days:
            raise ValueError(f"No valid data found for {from_date} – {to_date}")

        # Step 2: Build param combinations
        combos = [params_override] if params_override else generate_param_combinations(self.grid)
        total  = len(combos)

        logger.info(f"Combinations: {total:,}  |  Trading days: {len(loaded_days)}")
        logger.info(f"Total simulations: {total * len(loaded_days):,}")

        start = datetime.now()
        metrics_list = []

        for i, params in enumerate(combos):
            sim     = DaySimulator(params)
            results = [sim.simulate(day) for _, day in loaded_days]
            m       = compute_metrics(results, params.to_dict())
            m["combo_idx"] = i + 1
            metrics_list.append(m)

            if (i + 1) % max(1, total // 20) == 0 or i == 0:
                logger.info(f"  [{i+1}/{total}] PnL={m['total_pnl']:,.0f}  "
                            f"WR={m['win_rate_pct']}%  Sharpe={m['sharpe']}  "
                            f"Params: {params}")

        elapsed = (datetime.now() - start).total_seconds()
        logger.info(f"Grid done in {elapsed:.1f}s  ({elapsed/max(total,1):.2f}s/combo)")

        ranked = rank_param_sets(metrics_list)
        return GridRunResult(ranked, [d for d, _ in loaded_days], from_date, to_date)


class GridRunResult:
    def __init__(self, ranked, trading_dates, from_date, to_date):
        self.ranked        = ranked
        self.trading_dates = trading_dates
        self.from_date     = from_date
        self.to_date       = to_date

    def top(self, n=10):
        return self.ranked.head(n)

    def best_params(self) -> StrategyParams:
        row = self.ranked.iloc[0]
        p   = StrategyParams()
        for col in p.to_dict().keys():
            if col in row:
                setattr(p, col, row[col])
        return p

    def print_summary(self, n=10):
        print(f"\n{'='*80}")
        print(f"  GRID SEARCH RESULTS  |  {self.from_date} → {self.to_date}")
        print(f"  Total combinations: {len(self.ranked)}")
        print(f"{'='*80}")
        cols = ["rank", "total_pnl", "win_rate_pct", "sharpe", "max_drawdown",
                "profit_factor", "traded_days",
                "atr_timeframe", "atr_period", "atr_multiplier",
                "hedge_pct", "hedge_trail_step", "eod_exit_time"]
        avail = [c for c in cols if c in self.ranked.columns]
        print(self.ranked[avail].head(n).to_string(index=False))
        print()
