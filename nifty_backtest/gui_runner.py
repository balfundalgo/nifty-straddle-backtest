"""
gui_runner.py — Backtest engine bridge for GUI.
Direct imports, background thread, real-time progress, trade log CSV export.
"""

import sys
import os
import csv
from datetime import datetime
from typing import Callable, Optional, Dict, Any
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def run_backtest(cfg: dict,
                 progress_fn: Callable,
                 results_fn: Callable) -> Optional[Dict[str, Any]]:
    """
    Run backtest. Returns dict with 'report' and 'trade_log' paths.
    progress_fn(float 0-1)  — updates progress bar
    results_fn(metrics, daily_rows) — updates results tab
    """
    from config import StrategyParams, GridConfig
    from data_loader import DataLoader, PathConfig
    from day_simulator import DaySimulator
    from metrics import compute_metrics, rank_param_sets
    from report import generate_report

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
        slippage_pct                = float(cfg.get("slippage_pct", 0.001)),
        vix_low                     = float(cfg.get("vix_low", 12.0)),
        vix_mid_low                 = float(cfg.get("vix_mid_low", 16.0)),
        vix_mid_high                = float(cfg.get("vix_mid_high", 20.0)),
        sl_pct_vix_lt12             = float(cfg.get("sl_pct_vix_lt12", 0.40)),
        sl_pct_vix_12_16_calm       = float(cfg.get("sl_pct_vix_12_16_calm", 0.40)),
        sl_pct_vix_12_16_volatile   = float(cfg.get("sl_pct_vix_12_16_volatile", 0.25)),
        sl_pct_vix_16_20            = float(cfg.get("sl_pct_vix_16_20", 0.25)),
        sl_pct_vix_gt20             = float(cfg.get("sl_pct_vix_gt20", 0.15)),
    )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    print(f"{'═'*52}")
    print(f"  Mode  : {'Single Run' if mode == 'single' else 'Grid Search'}")
    print(f"  Range : {from_date} → {to_date}")
    print(f"  Params: {params}")
    print(f"{'═'*52}")

    # Load data
    # Load data — pass live callbacks so GUI console shows progress in real time
    loader = DataLoader(paths)
    loaded = loader.preload_all(
        from_date, to_date,
        log_fn=lambda msg: print(msg),
        progress_fn=progress_fn,
    )
    if not loaded:
        print(f"❌ No valid data found for {from_date} – {to_date}")
        return None

    progress_fn(0.1)

    # ── Single Run ────────────────────────────────────────────────────────
    if mode == "single":
        sim     = DaySimulator(params)
        results = []
        total   = len(loaded)

        for i, (date_str, day) in enumerate(loaded):
            res  = sim.simulate(day)
            icon = "✅" if res.status == "ok" else "⚠️ "
            pnl  = f"₹{res.total_pnl:>10,.2f}" if res.status == "ok" else "  (skipped)"
            print(f"  {date_str}  {icon}  {pnl}  "
                  f"ATM={res.atm_strike or '-':>6}  "
                  f"CE={res.ce_exit_reason or '-':>12}  "
                  f"PE={res.pe_exit_reason or '-':>12}"
                  + (f"  ({res.notes})" if res.notes else ""))
            results.append(res)
            progress_fn(0.1 + 0.8 * (i + 1) / total)

        m = compute_metrics(results, params.to_dict())
        _print_metrics(m)

        report_path    = f"single_run_{from_date}_{to_date}.xlsx"
        trade_log_path = f"trade_log_{from_date}_{to_date}_{ts}.csv"

        generate_report(pd.DataFrame([m]), results, output_path=report_path)
        _write_trade_log(results, trade_log_path)

        daily_rows = [r.to_dict() for r in results]
        results_fn(m, daily_rows)
        progress_fn(1.0)
        return {"report": report_path, "trade_log": trade_log_path}

    # ── Grid Search ───────────────────────────────────────────────────────
    elif mode in ("grid", "Grid Search"):
        from grid_runner import generate_param_combinations

        grid = GridConfig()

        if cfg.get("fast"):
            print("⚡ Fast Grid mode")
            grid.atr_timeframes   = ["5min"]
            grid.atr_periods      = [14]
            grid.atr_multipliers  = [1.0, 1.5]
            grid.eod_exit_times   = ["15:20"]
        else:
            def parse_str(k):  return [x.strip() for x in cfg[k].split(",") if x.strip()]
            def parse_int(k):  return [int(x.strip()) for x in cfg[k].split(",") if x.strip()]
            def parse_flt(k):  return [float(x.strip()) for x in cfg[k].split(",") if x.strip()]
            grid.atm_scan_starts          = parse_str("g_atm_start")
            grid.atm_scan_ends            = parse_str("g_atm_end")
            grid.max_premium_diffs        = parse_flt("g_prem_diff")
            grid.hedge_pcts               = parse_flt("g_hedge_pct")
            grid.hedge_trail_steps        = parse_flt("g_trail_step")
            grid.vix_intraday_thresholds  = parse_flt("g_vix_thr")
            grid.atr_timeframes           = parse_str("g_atr_tf")
            grid.atr_periods              = parse_int("g_atr_per")
            grid.atr_multipliers          = parse_flt("g_atr_mult")
            grid.eod_exit_times           = parse_str("g_eod")
            grid.vix_lows                 = parse_flt("g_vix_low")
            grid.vix_mid_lows             = parse_flt("g_vix_mid_low")
            grid.vix_mid_highs            = parse_flt("g_vix_mid_high")
            grid.sl_pct_lt12_list         = parse_flt("g_sl_lt12")
            grid.sl_pct_12_16_calm_list   = parse_flt("g_sl_calm")
            grid.sl_pct_12_16_vol_list    = parse_flt("g_sl_vol")
            grid.sl_pct_16_20_list        = parse_flt("g_sl_1620")
            grid.sl_pct_gt20_list         = parse_flt("g_sl_gt20")

        combos = generate_param_combinations(grid)
        total  = len(combos)
        days   = len(loaded)
        print(f"🔢 Grid: {total:,} combinations × {days} days = {total*days:,} simulations\n")

        metrics_list = []
        for i, p in enumerate(combos):
            sim     = DaySimulator(p)
            results = [sim.simulate(day) for _, day in loaded]
            m       = compute_metrics(results, p.to_dict())
            m["combo_idx"] = i + 1
            metrics_list.append(m)
            progress_fn(0.1 + 0.8 * (i + 1) / total)

            if (i + 1) % max(1, total // 20) == 0 or i == 0:
                print(f"  [{i+1:>4}/{total}]  PnL=₹{m['total_pnl']:>10,.0f}  "
                      f"WR={m['win_rate_pct']:>5.1f}%  Sharpe={m['sharpe']:>6.3f}  "
                      f"ATR[{p.atr_timeframe},p{p.atr_period},x{p.atr_multiplier}]")

        ranked = rank_param_sets(metrics_list)

        print(f"\n{'═'*52}")
        print("  TOP 10 COMBINATIONS")
        print(f"{'═'*52}")
        for _, row in ranked.head(10).iterrows():
            print(f"  #{int(row['rank']):<3}  PnL=₹{row['total_pnl']:>10,.0f}  "
                  f"WR={row['win_rate_pct']:>5.1f}%  Sharpe={row['sharpe']:>6.3f}  "
                  f"ATR[{row.get('atr_timeframe','?')},p{row.get('atr_period','?')},"
                  f"x{row.get('atr_multiplier','?')}]  EOD={row.get('eod_exit_time','?')}")

        # Rerun best params for daily detail
        best = StrategyParams()
        for col in best.to_dict().keys():
            if col in ranked.columns:
                setattr(best, col, ranked.iloc[0][col])

        print(f"\n▶  Rerunning best params for daily report...")
        sim        = DaySimulator(best)
        best_daily = [sim.simulate(day) for _, day in loaded]

        report_path    = f"grid_search_{from_date}_{to_date}.xlsx"
        trade_log_path = f"trade_log_best_{from_date}_{to_date}_{ts}.csv"

        generate_report(ranked, best_daily, output_path=report_path)
        _write_trade_log(best_daily, trade_log_path)

        best_m     = compute_metrics(best_daily, best.to_dict())
        daily_rows = [r.to_dict() for r in best_daily]
        _print_metrics(best_m)
        results_fn(best_m, daily_rows)
        progress_fn(1.0)
        return {"report": report_path, "trade_log": trade_log_path}

    return None


def _write_trade_log(results, path: str):
    """
    Write detailed CSV trade log — one row per trading day with full strike details,
    entry/exit prices, SL levels, P&L breakdown.
    """
    fieldnames = [
        "date", "expiry", "status",
        "entry_time",
        # ATM sell
        "atm_strike",
        "ce_entry", "ce_sl", "ce_exit", "ce_exit_reason", "ce_exit_time",
        "pe_entry", "pe_sl", "pe_exit", "pe_exit_reason", "pe_exit_time",
        # Hedge
        "ce_hedge_strike", "ce_hedge_entry", "ce_hedge_exit", "ce_hedge_exit_reason",
        "pe_hedge_strike", "pe_hedge_entry", "pe_hedge_exit", "pe_hedge_exit_reason",
        # VIX
        "vix_at_entry",
        # P&L
        "ce_sell_pnl", "pe_sell_pnl",
        "ce_hedge_pnl", "pe_hedge_pnl",
        "total_pnl",
        "notes",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in results:
            row = r.to_dict()
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    print(f"📋 Trade log saved: {path}")


def _print_metrics(m: dict):
    print(f"\n{'─'*45}")
    print(f"  Total P&L:             ₹{m.get('total_pnl', 0):>12,.2f}")
    print(f"  Traded days:           {m.get('traded_days', 0)}")
    print(f"  Win rate:              {m.get('win_rate_pct', 0):.1f}%")
    print(f"  Avg daily P&L:         ₹{m.get('avg_daily_pnl', 0):>12,.2f}")
    print(f"  Sharpe (annual):       {m.get('sharpe', 0):.3f}")
    print(f"  Max drawdown:          ₹{m.get('max_drawdown', 0):>12,.2f}")
    print(f"  Profit factor:         {m.get('profit_factor', 0):.3f}")
    print(f"  Risk : Reward:         1 : {m.get('recovery_ratio', 0):.2f}")
    print(f"  Max consec profit days:{m.get('max_consec_wins', 0)}")
    print(f"  Max consec loss days:  {m.get('max_consec_losses', 0)}")
    print(f"  Both legs SL hit:      {m.get('both_legs_sl', 0)}")
    print(f"  One leg SL hit:        {m.get('one_leg_sl', 0)}")
    print(f"  EOD exits:             {m.get('eod_exits', 0)}")
    print(f"{'─'*45}")
