"""
gui_runner.py — Connects GUI to backtest engine.
Direct imports, runs in background thread, no subprocess.
"""

import sys
import os
import logging
from typing import Callable, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def run_backtest(cfg: dict, progress_fn: Callable) -> Optional[str]:
    """
    Run backtest from GUI config dict.
    progress_fn(float 0-1) — called to update progress bar.
    Returns path to Excel report, or None on failure.
    """
    from config import StrategyParams, GridConfig
    from data_loader import DataLoader, PathConfig
    from day_simulator import DaySimulator
    from metrics import compute_metrics, rank_param_sets
    from report import generate_report
    import pandas as pd

    mode      = cfg["mode"]
    from_date = cfg["from_date"]
    to_date   = cfg["to_date"]
    paths     = PathConfig(base_path=cfg["data_path"])

    params = StrategyParams(
        atm_scan_start         = cfg["atm_scan_start"],
        atm_scan_end           = cfg["atm_scan_end"],
        max_premium_diff       = cfg["max_premium_diff"],
        hedge_pct              = cfg["hedge_pct"],
        vix_intraday_threshold = cfg["vix_intraday_threshold"],
        sl_buffer              = cfg["sl_buffer"],
        atr_timeframe          = cfg["atr_timeframe"],
        atr_period             = cfg["atr_period"],
        atr_multiplier         = cfg["atr_multiplier"],
        hedge_trail_step       = cfg["hedge_trail_step"],
        eod_exit_time          = cfg["eod_exit_time"],
        lot_size               = cfg["lot_size"],
    )

    print(f"{'═'*50}")
    print(f"  Mode : {mode}  |  {from_date} → {to_date}")
    print(f"  {params}")
    print(f"{'═'*50}")

    # Load data
    loader = DataLoader(paths)
    loaded = loader.preload_all(from_date, to_date)
    if not loaded:
        print(f"❌ No valid data found for {from_date} – {to_date}")
        return None

    print(f"✅ Loaded {len(loaded)} valid trading days")
    progress_fn(0.1)

    # ── Single Run ────────────────────────────────────────────────────────
    if mode == "Single Run":
        sim     = DaySimulator(params)
        results = []
        total   = len(loaded)

        for i, (date_str, day) in enumerate(loaded):
            res  = sim.simulate(day)
            icon = "✅" if res.status == "ok" else "⚠️ "
            print(f"  {date_str} {icon}  PnL={res.total_pnl:>10,.2f}  "
                  f"ATM={res.atm_strike}  "
                  f"CE={res.ce_exit_reason or '-'}  PE={res.pe_exit_reason or '-'}"
                  f"{'  ('+res.notes+')' if res.notes else ''}")
            results.append(res)
            progress_fn(0.1 + 0.8 * (i + 1) / total)

        m = compute_metrics(results, params.to_dict())
        _print_metrics(m)
        report_path = f"single_run_{from_date}_{to_date}.xlsx"
        generate_report(pd.DataFrame([m]), results, output_path=report_path)
        return report_path

    # ── Grid / Fast Grid ──────────────────────────────────────────────────
    elif mode in ("Grid Search", "Fast Grid"):
        from grid_runner import generate_param_combinations
        grid = GridConfig()

        if mode == "Fast Grid":
            print("Fast Grid — reduced parameter set")
            grid.atr_timeframes   = ["5min"]
            grid.atr_periods      = [14]
            grid.atr_multipliers  = [1.0, 1.5]
            grid.eod_exit_times   = ["15:20"]
        else:
            grid.atr_timeframes   = [x.strip() for x in cfg["grid_atr_tf"].split(",")]
            grid.atr_periods      = [int(x.strip()) for x in cfg["grid_atr_per"].split(",")]
            grid.atr_multipliers  = [float(x.strip()) for x in cfg["grid_atr_mult"].split(",")]
            grid.hedge_pcts       = [float(x.strip()) for x in cfg["grid_hedge_pct"].split(",")]
            grid.eod_exit_times   = [x.strip() for x in cfg["grid_eod"].split(",")]
            grid.max_premium_diffs= [float(x.strip()) for x in cfg["grid_prem_diff"].split(",")]

        combos = generate_param_combinations(grid)
        total  = len(combos)
        print(f"Grid: {total:,} combinations × {len(loaded)} days = {total*len(loaded):,} simulations")

        metrics_list = []
        for i, p in enumerate(combos):
            sim     = DaySimulator(p)
            results = [sim.simulate(day) for _, day in loaded]
            m       = compute_metrics(results, p.to_dict())
            m["combo_idx"] = i + 1
            metrics_list.append(m)
            progress_fn(0.1 + 0.8 * (i + 1) / total)

            if (i + 1) % max(1, total // 10) == 0:
                print(f"  [{i+1}/{total}] PnL={m['total_pnl']:,.0f}  "
                      f"WR={m['win_rate_pct']}%  Sharpe={m['sharpe']}")

        ranked = rank_param_sets(metrics_list)

        print(f"\n{'═'*50}")
        print("  TOP 5 COMBINATIONS")
        print(f"{'═'*50}")
        for _, row in ranked.head(5).iterrows():
            print(f"  #{int(row['rank'])}  PnL=₹{row['total_pnl']:,.0f}  "
                  f"WR={row['win_rate_pct']}%  Sharpe={row['sharpe']}  "
                  f"ATR[{row.get('atr_timeframe','?')},p{row.get('atr_period','?')},"
                  f"x{row.get('atr_multiplier','?')}]")

        # Rerun best params for daily detail report
        best = StrategyParams()
        for col in best.to_dict().keys():
            if col in ranked.columns:
                setattr(best, col, ranked.iloc[0][col])

        sim        = DaySimulator(best)
        best_daily = [sim.simulate(day) for _, day in loaded]
        report_path = f"grid_search_{from_date}_{to_date}.xlsx"
        generate_report(ranked, best_daily, output_path=report_path)
        return report_path


def _print_metrics(m: dict):
    print(f"\n{'─'*45}")
    print(f"  Total P&L:         ₹{m.get('total_pnl', 0):>12,.2f}")
    print(f"  Traded days:       {m.get('traded_days', 0)}")
    print(f"  Win rate:          {m.get('win_rate_pct', 0):.1f}%")
    print(f"  Avg daily P&L:     ₹{m.get('avg_daily_pnl', 0):>12,.2f}")
    print(f"  Sharpe (annual):   {m.get('sharpe', 0):.3f}")
    print(f"  Max drawdown:      ₹{m.get('max_drawdown', 0):>12,.2f}")
    print(f"  Profit factor:     {m.get('profit_factor', 0):.3f}")
    print(f"  Max consec losses: {m.get('max_consec_losses', 0)}")
    print(f"{'─'*45}")
