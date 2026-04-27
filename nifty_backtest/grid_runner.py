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


import multiprocessing as _mp
from concurrent.futures import ProcessPoolExecutor, as_completed


def _run_combo_chunk(args):
    """
    Worker function for parallel grid search.
    Runs a list of StrategyParams combos against pre-loaded day data.
    Must be module-level for multiprocessing pickling.
    """
    combo_list, day_data_list, start_idx = args
    from day_simulator import DaySimulator
    from metrics import compute_metrics

    results = []
    for i, params in enumerate(combo_list):
        sim     = DaySimulator(params)
        day_res = [sim.simulate(day) for _, day in day_data_list]
        m       = compute_metrics(day_res, params.to_dict())
        m["combo_idx"] = start_idx + i + 1
        results.append(m)
    return results


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
        ("slippage_pct",              grid.slippage_pcts),
        ("hedge_trail_step",          grid.hedge_trail_steps),
        ("eod_exit_time",             grid.eod_exit_times),
        ("vix_low",                   grid.vix_lows),
        ("vix_mid_low",               grid.vix_mid_lows),
        ("vix_mid_high",              grid.vix_mid_highs),
        ("sl_pct_vix_lt12",           grid.sl_pct_lt12_list),
        ("sl_pct_vix_12_16_calm",     grid.sl_pct_12_16_calm_list),
        ("sl_pct_vix_12_16_volatile", grid.sl_pct_12_16_vol_list),
        ("sl_pct_vix_16_20",          grid.sl_pct_16_20_list),
        ("sl_pct_vix_gt20",           grid.sl_pct_gt20_list),  # singular key matches StrategyParams
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
        progress_fn=None,
    ) -> "GridRunResult":
        if progress_fn is None:
            progress_fn = lambda v: None

        # Step 1: Load all data into memory once
        loader = DataLoader(self.paths)
        logger.info("Preloading all trading day data...")
        loaded_days: List[Tuple[str, DayData]] = loader.preload_all(from_date, to_date, log_fn=lambda msg: print(msg), progress_fn=progress_fn)

        if not loaded_days:
            raise ValueError(f"No valid data found for {from_date} – {to_date}")

        # Step 2: Build param combinations
        combos = [params_override] if params_override else generate_param_combinations(self.grid)
        total  = len(combos)

        logger.info(f"Combinations: {total:,}  |  Trading days: {len(loaded_days)}")
        logger.info(f"Total simulations: {total * len(loaded_days):,}")

        start = datetime.now()
        # ── ATM Cache: group combos by ATM+hedge params ──────────────────────
        # ATM selection is the same for all combos sharing the same ATM params.
        # Compute it ONCE per unique group, reuse across all combos in that group.
        # This eliminates ~80% of computation time.
        from itertools import groupby
        from day_simulator import DaySimulator as _DS

        def _atm_key(p):
            return (p.atm_scan_start, p.atm_scan_end,
                    p.max_premium_diff, p.hedge_pct, p.slippage_pct)

        # Group and sort combos by ATM key
        sorted_combos  = sorted(combos, key=_atm_key)
        atm_groups     = {k: list(v) for k, v in groupby(sorted_combos, key=_atm_key)}
        n_atm_groups   = len(atm_groups)
        logger.info(f"ATM cache: {n_atm_groups} unique ATM groups for {total} combos")
        print(f"⚡ ATM cache: {n_atm_groups} groups × {n_days} days = "
              f"{n_atm_groups * n_days} ATM lookups (was {total * n_days:,})")

        # Pre-compute ATM+hedge entries per group
        atm_entry_cache = {}   # key → [entry_or_None per day]
        for key, group_combos in atm_groups.items():
            tmp_sim = _DS(group_combos[0])  # use first combo for ATM params
            entries = []
            for _, day in loaded_days:
                try:
                    e = tmp_sim.compute_day_entry(day)
                except Exception:
                    e = None
                entries.append(e)
            atm_entry_cache[key] = entries

        print(f"  ATM cache built ✅")
        progress_fn(0.15)

        # ── Run all combos using cached entries ────────────────────────────
        metrics_list = []
        completed    = 0
        n_cores      = max(1, _mp.cpu_count() - 1)
        print(f"⚡ Running {total} combos on {n_cores} cores...")

        def _run_group_chunk(args):
            """Worker: run a list of (combo, entries_list) pairs."""
            combo_entries_list, day_data_list, start_idx = args
            from day_simulator import DaySimulator
            from metrics import compute_metrics
            results = []
            for i, (params, entries) in enumerate(combo_entries_list):
                sim     = DaySimulator(params)
                day_res = []
                for j, (_, day) in enumerate(day_data_list):
                    entry = entries[j]
                    if entry is not None:
                        res = sim.simulate_with_entry(day, entry)
                    else:
                        res = sim.simulate(day)   # fallback if entry missing
                    day_res.append(res)
                m = compute_metrics(day_res, params.to_dict())
                m["combo_idx"] = start_idx + i + 1
                results.append(m)
            return results

        # Build flat list of (combo, entries) preserving original combo_idx
        all_combo_entries = []
        for combo in combos:
            key     = _atm_key(combo)
            entries = atm_entry_cache[key]
            all_combo_entries.append((combo, entries))

        chunk_size = max(1, total // (n_cores * 4))
        chunks = []
        for start in range(0, total, chunk_size):
            end   = min(start + chunk_size, total)
            chunk = all_combo_entries[start:end]
            chunks.append((chunk, loaded_days, start))

        with ProcessPoolExecutor(max_workers=n_cores) as executor:
            futures = {executor.submit(_run_group_chunk, chunk): i
                       for i, chunk in enumerate(chunks)}
            for future in as_completed(futures):
                try:
                    chunk_results = future.result()
                    metrics_list.extend(chunk_results)
                    completed += len(chunk_results)
                    progress_fn(0.15 + 0.8 * completed / total)
                    if completed % max(1, total // 10) == 0 or completed == total:
                        best = max(metrics_list, key=lambda x: x.get("total_pnl", 0))
                        print(f"  [{completed}/{total}] Best: "
                              f"PnL=₹{best['total_pnl']:,.0f}  "
                              f"WR={best.get('win_rate_pct','?')}%  "
                              f"Sharpe={best.get('sharpe','?')}")
                except Exception as e:
                    logger.error(f"Chunk failed: {e}")
                    import traceback
                    traceback.print_exc()

        metrics_list.sort(key=lambda x: x.get("combo_idx", 0))


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
