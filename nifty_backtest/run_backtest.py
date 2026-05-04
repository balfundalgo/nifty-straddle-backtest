"""
run_backtest.py
===============
Fastest possible NIFTY straddle backtest — run directly in VSCode.

USAGE:
    python run_backtest.py              # Single run with default params
    python run_backtest.py grid         # Grid search
    python run_backtest.py fast         # Fast grid (small combo set)

OUTPUT:
    backtest_result.xlsx    — full Excel report
    trade_log_YYYYMMDD.csv  — per-day trade details
"""

import sys
import time
import os
from datetime import datetime

# ── CONFIG — Edit these ───────────────────────────────────────────────────────
DATA_PATH   = r"C:\Users\Admin\Downloads\BreezeDownloader-v1.5.0\breeze_data"
FROM_DATE   = "2026-01-02"
TO_DATE     = "2026-04-21"

# Single run params
ATM_SCAN_START          = "09:16"
ATM_SCAN_END            = "09:21"
MAX_PREMIUM_DIFF        = 20.0
HEDGE_PCT               = 0.05
HEDGE_TRAIL_STEP        = 3.0
VIX_INTRADAY_THRESHOLD  = 3.0
ATR_TIMEFRAME           = "5min"
ATR_PERIOD              = 14
ATR_MULTIPLIER          = 1.5
EOD_EXIT_TIME           = "15:20"
LOT_SIZE                = 75
SLIPPAGE_PCT            = 0.001
SL_BUFFER               = 5.0
SL_PCT_VIX_R1           = 0.40
SL_PCT_VIX_R2_CALM      = 0.40
SL_PCT_VIX_R2_VOLATILE  = 0.25
SL_PCT_VIX_R3           = 0.25
SL_PCT_VIX_R4           = 0.15

# Grid search ranges
# Keys MUST match StrategyParams attribute names exactly
GRID = {
    "atr_timeframe":     ["5min", "15min"],
    "atr_period":        [7, 14, 21],
    "atr_multiplier":    [1.0, 1.5, 2.0],
    "eod_exit_time":     ["15:15", "15:20", "15:25"],
    "sl_pct_vix_r1":   [0.35, 0.40, 0.45],
    "sl_pct_vix_r4":   [0.10, 0.15, 0.20],
    # Add/remove grid params as needed. Available keys:
    # atr_timeframe, atr_period, atr_multiplier, eod_exit_time,
    # sl_pct_vix_lt12, sl_pct_vix_12_16_calm, sl_pct_vix_12_16_volatile,
    # sl_pct_vix_16_20, sl_pct_vix_gt20, hedge_pct, hedge_trail_step,
    # vix_intraday_threshold, max_premium_diff, slippage_pct
}

# ── FAST GRID — Small combo set for quick testing ─────────────────────────────
FAST_GRID = {
    "atr_timeframe":  ["5min"],
    "atr_period":     [14],
    "atr_multiplier": [1.0, 1.5, 2.0],
    "eod_exit_time":  ["15:20"],
}

# ─────────────────────────────────────────────────────────────────────────────

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import StrategyParams, GridConfig
from data_loader import DataLoader, PathConfig
from day_simulator import DaySimulator
from metrics import compute_metrics, rank_param_sets
from report import generate_report
from itertools import product, groupby
import csv
import pandas as pd


def _worker_chunk(args):
    """
    Module-level worker for ProcessPoolExecutor.
    Receives a chunk of (combo_idx, params_dict, entries) and loaded days.
    Returns list of metrics dicts.
    """
    work_items, loaded_days = args
    from day_simulator import DaySimulator
    from metrics import compute_metrics

    results_list = []
    for combo_idx, params_dict, entries in work_items:
        p = make_params(**{k: params_dict[k] for k in params_dict
                           if k not in ("atm_scan_start","atm_scan_end",
                                        "max_premium_diff","hedge_pct",
                                        "slippage_pct")})
        # Apply all params from dict
        for k, v in params_dict.items():
            setattr(p, k, v)

        sim     = DaySimulator(p)
        results = []
        for j, (_, day) in enumerate(loaded_days):
            e = entries[j]
            results.append(sim.simulate_with_entry(day, e)
                           if e is not None else sim.simulate(day))

        m = compute_metrics(results, params_dict)
        m["combo_idx"] = combo_idx + 1
        results_list.append(m)

    return results_list


def make_params(**overrides) -> StrategyParams:
    """Build StrategyParams from defaults + overrides."""
    p = StrategyParams(
        atm_scan_start          = ATM_SCAN_START,
        atm_scan_end            = ATM_SCAN_END,
        max_premium_diff        = MAX_PREMIUM_DIFF,
        hedge_pct               = HEDGE_PCT,
        hedge_trail_step        = HEDGE_TRAIL_STEP,
        vix_intraday_threshold  = VIX_INTRADAY_THRESHOLD,
        atr_timeframe           = ATR_TIMEFRAME,
        atr_period              = ATR_PERIOD,
        atr_multiplier          = ATR_MULTIPLIER,
        eod_exit_time           = EOD_EXIT_TIME,
        lot_size                = LOT_SIZE,
        slippage_pct            = SLIPPAGE_PCT,
        sl_buffer               = SL_BUFFER,
        sl_pct_vix_r1           = SL_PCT_VIX_R1,
        sl_pct_vix_r2_calm      = SL_PCT_VIX_R2_CALM,
        sl_pct_vix_r2_volatile  = SL_PCT_VIX_R2_VOLATILE,
        sl_pct_vix_r3           = SL_PCT_VIX_R3,
        sl_pct_vix_r4           = SL_PCT_VIX_R4,
    )
    for k, v in overrides.items():
        setattr(p, k, v)
    return p


def load_data():
    """Load all trading days into memory."""
    print(f"\n📂 Loading data: {FROM_DATE} → {TO_DATE}")
    t0 = time.time()
    loader = DataLoader(PathConfig(base_path=DATA_PATH))
    loaded = loader.preload_all(
        FROM_DATE, TO_DATE,
        log_fn=print,
        progress_fn=lambda v: None
    )
    print(f"✅ {len(loaded)} days loaded in {time.time()-t0:.1f}s\n")
    return loaded


def run_single(loaded):
    """Single backtest run with default params."""
    params  = make_params()
    sim     = DaySimulator(params)
    results = []
    total   = len(loaded)

    print(f"{'─'*60}")
    print(f"  SINGLE RUN  |  {params}")
    print(f"{'─'*60}")

    t0 = time.time()
    for i, (date_str, day) in enumerate(loaded):
        res  = sim.simulate(day)
        icon = "✅" if res.status == "ok" else "⚠️ "
        pnl  = f"₹{res.total_pnl:>10,.2f}" if res.status == "ok" else "  (skipped)  "
        note = f"  [{res.notes}]" if res.notes else ""
        print(f"  {date_str}  {icon}  {pnl}  ATM={res.atm_strike or '-':>6}  "
              f"CE={res.ce_exit_reason or '-':>12}  "
              f"PE={res.pe_exit_reason or '-':>12}{note}")
        results.append(res)

    elapsed = time.time() - t0
    print(f"\n⏱  {elapsed:.1f}s total ({elapsed/total:.3f}s/day)")

    m = compute_metrics(results, params.to_dict())
    _print_metrics(m)

    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"single_run_{FROM_DATE}_{TO_DATE}.xlsx"
    log_path    = f"trade_log_{ts}.csv"

    generate_report(pd.DataFrame([m]), results, output_path=report_path)
    _write_trade_log(results, log_path)
    print(f"\n📊 Report : {report_path}")
    print(f"📋 Trade log: {log_path}")


def run_grid(loaded, grid_dict):
    """Grid search with ATM caching for maximum speed."""

    # Build all combinations
    keys   = list(grid_dict.keys())
    values = list(grid_dict.values())
    combos = []
    for vals in product(*values):
        overrides = dict(zip(keys, vals))
        combos.append(make_params(**overrides))

    total  = len(combos)
    n_days = len(loaded)
    print(f"{'─'*60}")
    print(f"  GRID SEARCH  |  {total} combos × {n_days} days = {total*n_days:,} sims")
    print(f"{'─'*60}\n")

    # ── ATM cache: compute entries once per unique ATM+hedge group ────────
    def atm_key(p):
        return (p.atm_scan_start, p.atm_scan_end,
                p.max_premium_diff, p.hedge_pct, p.slippage_pct)

    atm_groups = {k: list(v) for k, v in groupby(
        sorted(combos, key=atm_key), key=atm_key)}
    n_groups = len(atm_groups)
    print(f"⚡ ATM cache: {n_groups} groups × {n_days} days "
          f"= {n_groups*n_days} lookups (was {total*n_days:,})")

    t0 = time.time()
    atm_cache = {}
    for gi, (key, grp) in enumerate(atm_groups.items()):
        tmp = DaySimulator(grp[0])
        entries = []
        for _, day in loaded:
            try:    e = tmp.compute_day_entry(day)
            except: e = None
            entries.append(e)
        atm_cache[key] = entries
        if (gi + 1) % max(1, n_groups // 5) == 0 or gi == n_groups - 1:
            print(f"  Cache [{gi+1}/{n_groups}] built...")
    print(f"✅ ATM cache ready in {time.time()-t0:.1f}s\n")

    # ── Parallel simulation using ThreadPoolExecutor ─────────────────────
    # Threads share memory — no serialization of large data needed.
    # Numpy releases GIL so threads get real parallelism on heavy computation.
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    n_cores  = max(1, __import__('multiprocessing').cpu_count() - 1)
    print_lock = threading.Lock()
    print(f"⚡ Running on {n_cores} threads...\n")

    def run_combo(args):
        i, p, entries = args
        # p is the actual StrategyParams object with correct params
        sim     = DaySimulator(p)
        results = []
        for j, (_, day) in enumerate(loaded):
            e = entries[j]
            results.append(sim.simulate_with_entry(day, e)
                           if e is not None else sim.simulate(day))
        m = compute_metrics(results, p.to_dict())
        m["combo_idx"] = i + 1
        # Store key params in metrics for display
        m["_atr_timeframe"] = p.atr_timeframe
        m["_atr_period"]    = p.atr_period
        m["_atr_multiplier"]= p.atr_multiplier
        m["_eod_exit_time"] = p.eod_exit_time
        return i, m, p

    work = [(i, p, atm_cache[atm_key(p)]) for i, p in enumerate(combos)]

    metrics_list = [None] * total
    completed    = 0
    t1           = time.time()

    with ThreadPoolExecutor(max_workers=n_cores) as executor:
        futures = {executor.submit(run_combo, w): w[0] for w in work}
        for future in as_completed(futures):
            i, m, p = future.result()
            metrics_list[i] = m
            completed += 1
            elapsed = time.time() - t1
            eta = elapsed / completed * (total - completed) if completed > 0 else 0
            with print_lock:
                print(f"  [{completed:>4}/{total}]  "
                      f"PnL=₹{m['total_pnl']:>10,.0f}  "
                      f"WR={m['win_rate_pct']:>5.1f}%  "
                      f"Sharpe={m['sharpe']:>6.3f}  "
                      f"ATR[{p.atr_timeframe},p{p.atr_period},x{p.atr_multiplier}]  "
                      f"EOD={p.eod_exit_time}  "
                      f"SL%>{p.sl_pct_vix_gt20}  "
                      f"ETA={eta/60:.1f}min")

    metrics_list = [m for m in metrics_list if m is not None]

    print(f"\n⏱  Grid done in {time.time()-t0:.1f}s")

    ranked = rank_param_sets(metrics_list)
    print(f"\n{'═'*60}")
    print("  TOP 10 COMBINATIONS")
    print(f"{'═'*60}")
    for _, row in ranked.head(10).iterrows():
        print(f"  #{int(row['rank']):<3}  "
              f"PnL=₹{row['total_pnl']:>10,.0f}  "
              f"WR={row['win_rate_pct']:>5.1f}%  "
              f"Sharpe={row['sharpe']:>6.3f}  "
              f"ATR[{row.get('atr_timeframe','?')},"
              f"p{row.get('atr_period','?')},"
              f"x{row.get('atr_multiplier','?')}]  "
              f"EOD={row.get('eod_exit_time','?')}")

    # Rerun best for daily report
    best = ranked.iloc[0]
    best_p = make_params(**{k: best[k] for k in p.to_dict() if k in best})
    sim        = DaySimulator(best_p)
    best_daily = [sim.simulate_with_entry(day, atm_cache[atm_key(best_p)][j])
                  if atm_cache[atm_key(best_p)][j] is not None
                  else sim.simulate(day)
                  for j, (_, day) in enumerate(loaded)]

    ts          = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"grid_search_{FROM_DATE}_{TO_DATE}.xlsx"
    log_path    = f"trade_log_best_{ts}.csv"
    generate_report(ranked, best_daily, output_path=report_path)
    _write_trade_log(best_daily, log_path)
    print(f"\n📊 Report : {report_path}")
    print(f"📋 Trade log: {log_path}")


def _print_metrics(m):
    print(f"\n{'─'*45}")
    print(f"  Total P&L:            ₹{m.get('total_pnl',0):>12,.2f}")
    print(f"  Traded days:          {m.get('traded_days',0)}")
    print(f"  Win rate:             {m.get('win_rate_pct',0):.1f}%")
    print(f"  Avg daily P&L:        ₹{m.get('avg_daily_pnl',0):>12,.2f}")
    print(f"  Sharpe (annual):      {m.get('sharpe',0):.3f}")
    print(f"  Std Dev (daily):      ₹{m.get('std_daily_pnl',0):>12,.2f}")
    print(f"  Max drawdown:         ₹{m.get('max_drawdown',0):>12,.2f}")
    print(f"  Profit factor:        {m.get('profit_factor',0):.3f}")
    print(f"  Risk:Reward:          1 : {m.get('recovery_ratio',0):.2f}")
    print(f"  Max consec profit:    {m.get('max_consec_wins',0)} days")
    print(f"  Max consec loss:      {m.get('max_consec_losses',0)} days")
    print(f"{'─'*45}")


def _write_trade_log(results, path):
    fields = [
        "date","expiry","status","entry_time","atm_strike",
        "ce_entry","ce_sl","ce_exit","ce_exit_reason","ce_exit_time",
        "pe_entry","pe_sl","pe_exit","pe_exit_reason","pe_exit_time",
        "ce_hedge_strike","ce_hedge_entry","ce_hedge_exit","ce_hedge_exit_reason",
        "pe_hedge_strike","pe_hedge_entry","pe_hedge_exit","pe_hedge_exit_reason",
        "vix_at_entry","total_pnl","notes",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in results:
            w.writerow({k: getattr(r, k, "") for k in fields})
    print(f"📋 Trade log: {path}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "single"
    loaded = load_data()

    if mode == "single":
        run_single(loaded)
    elif mode == "grid":
        run_grid(loaded, GRID)
    elif mode == "fast":
        run_grid(loaded, FAST_GRID)
    else:
        print(f"Unknown mode: {mode}. Use: single / grid / fast")
