"""
main.py — Entry point for the NIFTY Straddle Backtest System.

Commands:
    python main.py single  --from 2024-01-01 --to 2024-03-31
    python main.py grid    --from 2024-01-01 --to 2024-03-31 [--fast]
    python main.py stats   --from 2024-01-01 --to 2024-03-31
"""

import argparse
import logging
import sys
from typing import List

from config import StrategyParams, GridConfig
from data_loader import DataLoader, PathConfig
from day_simulator import DaySimulator, DayResult
from grid_runner import GridRunner
from metrics import compute_metrics, results_to_df
from report import generate_report

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("backtest.log", mode="a"),
    ],
)
logger = logging.getLogger("main")

# ── Default data path (edit here or pass --data-path) ────────────────────────
DEFAULT_DATA_PATH = r"C:\Users\Admin\Downloads\BreezeDownloader-v1.4.2\breeze_data"


def make_path_config(args) -> PathConfig:
    return PathConfig(base_path=getattr(args, "data_path", DEFAULT_DATA_PATH))


# ─────────────────────────────────────────────────────────────────────────────

def cmd_single(args):
    paths  = make_path_config(args)
    params = StrategyParams()

    if args.atm_start:   params.atm_scan_start       = args.atm_start
    if args.atm_end:     params.atm_scan_end         = args.atm_end
    if args.eod:         params.eod_exit_time        = args.eod
    if args.atr_tf:      params.atr_timeframe        = args.atr_tf
    if args.atr_period:  params.atr_period           = args.atr_period
    if args.atr_mult:    params.atr_multiplier       = args.atr_mult
    if args.hedge_pct:   params.hedge_pct            = args.hedge_pct
    if args.trail_step:  params.hedge_trail_step     = args.trail_step

    print(f"\n{'='*60}")
    print(f"  SINGLE BACKTEST RUN")
    print(f"  {args.from_date} → {args.to_date}")
    print(f"  Params: {params}")
    print(f"{'='*60}")

    loader = DataLoader(paths)
    loaded = loader.preload_all(args.from_date, args.to_date)

    if not loaded:
        print(f"No valid data found for {args.from_date} – {args.to_date}")
        sys.exit(1)

    sim     = DaySimulator(params)
    results: List[DayResult] = []

    for date_str, day in loaded:
        res = sim.simulate(day)
        icon = "✅" if res.status == "ok" else "⚠️ "
        print(f"  {date_str}  {icon}  PnL={res.total_pnl:>10,.2f}  "
              f"ATM={res.atm_strike}  "
              f"CE={res.ce_exit_reason or '-'}  PE={res.pe_exit_reason or '-'}  "
              f"{'('+res.notes+')' if res.notes else ''}")
        results.append(res)

    m = compute_metrics(results, params.to_dict())
    _print_metrics(m)

    import pandas as pd
    generate_report(
        pd.DataFrame([m]), results,
        output_path=f"single_run_{args.from_date}_{args.to_date}.xlsx"
    )


def cmd_grid(args):
    paths = make_path_config(args)
    grid  = GridConfig()

    if args.fast:
        print("Fast mode: reduced grid")
        grid.atr_timeframes  = ["5min"]
        grid.atr_periods     = [14]
        grid.atr_multipliers = [1.0, 1.5]
        grid.eod_exit_times  = ["15:20"]

    total_combos = grid.total_combinations()
    print(f"\n{'='*60}")
    print(f"  GRID SEARCH")
    print(f"  {args.from_date} → {args.to_date}")
    print(f"  Combinations: {total_combos:,}")
    print(f"{'='*60}\n")

    runner = GridRunner(paths, grid)
    run_result = runner.run(from_date=args.from_date, to_date=args.to_date)
    run_result.print_summary(n=20)

    # Rerun best params to get daily detail
    best_params = run_result.best_params()
    loader      = DataLoader(paths)
    loaded      = loader.preload_all(args.from_date, args.to_date)
    sim         = DaySimulator(best_params)
    best_daily  = [sim.simulate(day) for _, day in loaded]

    generate_report(
        run_result.ranked, best_daily,
        output_path=f"grid_search_{args.from_date}_{args.to_date}.xlsx"
    )


def cmd_stats(args):
    paths  = make_path_config(args)
    loader = DataLoader(paths)
    loader.stats(from_date=args.from_date, to_date=args.to_date)


# ─────────────────────────────────────────────────────────────────────────────

def _print_metrics(m: dict):
    print(f"\n{'─'*50}")
    print(f"  Total P&L:         ₹{m.get('total_pnl', 0):>12,.2f}")
    print(f"  Traded days:       {m.get('traded_days', 0)}")
    print(f"  Win rate:          {m.get('win_rate_pct', 0):.1f}%")
    print(f"  Avg daily P&L:     ₹{m.get('avg_daily_pnl', 0):>12,.2f}")
    print(f"  Sharpe (annual):   {m.get('sharpe', 0):.3f}")
    print(f"  Max drawdown:      ₹{m.get('max_drawdown', 0):>12,.2f}")
    print(f"  Profit factor:     {m.get('profit_factor', 0):.3f}")
    print(f"  Max consec losses: {m.get('max_consec_losses', 0)}")
    print(f"{'─'*50}\n")


def build_parser():
    parser = argparse.ArgumentParser(description="NIFTY Straddle Backtest System")
    parser.add_argument("--data-path", default=DEFAULT_DATA_PATH,
                        help="Root folder containing INDVIX_1SEC, NIFTY_SPOT_1SEC, NIFTY_OPTIONS_1SEC")
    sub = parser.add_subparsers(dest="command")

    sg = sub.add_parser("single", help="Single parameter backtest")
    sg.add_argument("--from",       dest="from_date", required=True)
    sg.add_argument("--to",         dest="to_date",   required=True)
    sg.add_argument("--atm-start",  default=None)
    sg.add_argument("--atm-end",    default=None)
    sg.add_argument("--eod",        default=None)
    sg.add_argument("--atr-tf",     default=None)
    sg.add_argument("--atr-period", type=int,   default=None)
    sg.add_argument("--atr-mult",   type=float, default=None)
    sg.add_argument("--hedge-pct",  type=float, default=None)
    sg.add_argument("--trail-step", type=float, default=None)

    gr = sub.add_parser("grid", help="Full grid search")
    gr.add_argument("--from",  dest="from_date", required=True)
    gr.add_argument("--to",    dest="to_date",   required=True)
    gr.add_argument("--fast",  action="store_true")

    st = sub.add_parser("stats", help="Show data availability stats")
    st.add_argument("--from", dest="from_date", default="2000-01-01")
    st.add_argument("--to",   dest="to_date",   default="2099-12-31")

    return parser


if __name__ == "__main__":
    parser = build_parser()
    args   = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    {"single": cmd_single, "grid": cmd_grid, "stats": cmd_stats}[args.command](args)
