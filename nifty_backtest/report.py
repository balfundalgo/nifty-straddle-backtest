"""
report.py — Generate Excel report from grid search and day-by-day results.

Output sheets:
  1. Summary          — Grid search ranked results
  2. Top10_Daily      — Day-by-day P&L for the best param set
  3. Equity_Curve     — Cumulative P&L per param set (top 10)
  4. SL_Analysis      — SL hit analysis per day
  5. Params_Guide     — Parameter legend
"""

import pandas as pd
import numpy as np
from typing import List, Dict
from datetime import datetime
from pathlib import Path

from day_simulator import DayResult
from metrics import results_to_df


def _xl_col_width(series: pd.Series, header: str) -> int:
    """Auto-fit column width."""
    max_len = max(series.astype(str).str.len().max(), len(header)) + 2
    return min(max_len, 40)


def generate_report(
    grid_ranked: pd.DataFrame,
    daily_results_best: List[DayResult],   # Daily results for best param set
    output_path: str = None,
) -> str:
    """
    Generate multi-sheet Excel report.
    Returns the path to the saved file.
    """
    if output_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"backtest_results_{ts}.xlsx"

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        wb = writer.book
        
        # ── Formats ──────────────────────────────────────────────────────────
        hdr_fmt = wb.add_format({
            "bold": True, "bg_color": "#1a1f36", "font_color": "#FFFFFF",
            "border": 1, "align": "center", "valign": "vcenter",
        })
        money_fmt  = wb.add_format({"num_format": "₹#,##0.00", "border": 1})
        pct_fmt    = wb.add_format({"num_format": "0.0%", "border": 1})
        int_fmt    = wb.add_format({"num_format": "#,##0",    "border": 1})
        dec_fmt    = wb.add_format({"num_format": "0.000",    "border": 1})
        text_fmt   = wb.add_format({"border": 1})
        green_fmt  = wb.add_format({"bg_color": "#d4efdf", "num_format": "₹#,##0.00", "border": 1})
        red_fmt    = wb.add_format({"bg_color": "#fadbd8", "num_format": "₹#,##0.00", "border": 1})
        title_fmt  = wb.add_format({
            "bold": True, "font_size": 14, "font_color": "#1a1f36"
        })

        # ═══════════════════════════════════════════════════════════════════
        # Sheet 1: Summary (all param combos ranked)
        # ═══════════════════════════════════════════════════════════════════
        ws1 = wb.add_worksheet("Summary")
        writer.sheets["Summary"] = ws1

        ws1.write(0, 0, "NIFTY Straddle Backtest — Grid Search Results", title_fmt)
        ws1.write(1, 0, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

        summary_cols = [
            "rank", "total_pnl", "win_rate_pct", "sharpe",
            "max_drawdown", "profit_factor", "avg_daily_pnl",
            "max_win", "max_loss", "traded_days",
            "atm_scan_start", "atm_scan_end", "max_premium_diff",
            "hedge_pct", "vix_intraday_threshold",
            "atr_timeframe", "atr_period", "atr_multiplier",
            "hedge_trail_step", "eod_exit_time",
        ]
        available_cols = [c for c in summary_cols if c in grid_ranked.columns]
        df_sum = grid_ranked[available_cols].copy()

        # Write header
        for col_idx, col_name in enumerate(df_sum.columns):
            ws1.write(3, col_idx, col_name.replace("_", " ").title(), hdr_fmt)

        # Write rows
        for row_idx, (_, row) in enumerate(df_sum.iterrows()):
            r = row_idx + 4
            for col_idx, col_name in enumerate(df_sum.columns):
                val = row[col_name]
                fmt = text_fmt
                if col_name in ["total_pnl", "avg_daily_pnl", "max_win", "max_loss",
                                 "max_drawdown"]:
                    fmt = green_fmt if (isinstance(val, (int, float)) and val >= 0) else red_fmt
                elif col_name in ["win_rate_pct"]:
                    fmt = pct_fmt
                    val = val / 100 if isinstance(val, (int, float)) else val
                elif col_name in ["sharpe", "profit_factor", "atr_multiplier", "hedge_pct"]:
                    fmt = dec_fmt
                elif col_name in ["traded_days", "rank", "atr_period"]:
                    fmt = int_fmt
                ws1.write(r, col_idx, val, fmt)

        # Auto-fit columns
        for col_idx, col_name in enumerate(df_sum.columns):
            ws1.set_column(col_idx, col_idx, max(12, len(col_name) + 2))

        ws1.freeze_panes(4, 1)
        ws1.autofilter(3, 0, 3 + len(df_sum), len(df_sum.columns) - 1)

        # ═══════════════════════════════════════════════════════════════════
        # Sheet 2: Daily Results (best param set)
        # ═══════════════════════════════════════════════════════════════════
        ws2 = wb.add_worksheet("Top1_Daily")
        writer.sheets["Top1_Daily"] = ws2

        ws2.write(0, 0, "Daily P&L — Best Parameter Set", title_fmt)

        daily_df = results_to_df(daily_results_best)
        if not daily_df.empty:
            show_cols = [
                "date", "status", "atm_strike",
                "ce_entry", "pe_entry", "ce_hedge_entry", "pe_hedge_entry",
                "ce_sl", "pe_sl", "vix_at_entry",
                "ce_exit", "ce_exit_reason", "ce_exit_time",
                "pe_exit", "pe_exit_reason", "pe_exit_time",
                "ce_hedge_exit", "ce_hedge_exit_reason",
                "pe_hedge_exit", "pe_hedge_exit_reason",
                "ce_sell_pnl", "pe_sell_pnl", "ce_hedge_pnl", "pe_hedge_pnl",
                "total_pnl",
            ]
            show_cols = [c for c in show_cols if c in daily_df.columns]
            df_daily = daily_df[show_cols].copy()

            # Cumulative PnL
            tradeable = df_daily[df_daily["status"] == "ok"].copy()
            df_daily["cumulative_pnl"] = 0.0
            if "total_pnl" in tradeable.columns:
                cum = tradeable["total_pnl"].cumsum()
                df_daily.loc[tradeable.index, "cumulative_pnl"] = cum

            for col_idx, col in enumerate(df_daily.columns):
                ws2.write(2, col_idx, col.replace("_", " ").title(), hdr_fmt)

            for r_idx, (_, row) in enumerate(df_daily.iterrows()):
                r = r_idx + 3
                for c_idx, col in enumerate(df_daily.columns):
                    val = row[col]
                    if pd.isna(val):
                        val = ""
                    if col in ["total_pnl", "cumulative_pnl", "ce_sell_pnl",
                                "pe_sell_pnl", "ce_hedge_pnl", "pe_hedge_pnl"]:
                        fmt = green_fmt if isinstance(val, float) and val >= 0 else red_fmt
                    else:
                        fmt = text_fmt
                    ws2.write(r, c_idx, val, fmt)

            for c_idx, col in enumerate(df_daily.columns):
                ws2.set_column(c_idx, c_idx, max(10, len(col) + 2))
            ws2.freeze_panes(3, 1)

        # ═══════════════════════════════════════════════════════════════════
        # Sheet 3: Equity Curve data (for charting)
        # ═══════════════════════════════════════════════════════════════════
        ws3 = wb.add_worksheet("Equity_Curve")
        writer.sheets["Equity_Curve"] = ws3

        ws3.write(0, 0, "Cumulative P&L (Best Param Set)", title_fmt)

        if not daily_df.empty and "total_pnl" in daily_df.columns:
            eq_df = daily_df[daily_df["status"] == "ok"][["date", "total_pnl"]].copy()
            eq_df["cumulative"] = eq_df["total_pnl"].cumsum()
            eq_df["drawdown"]   = eq_df["cumulative"] - eq_df["cumulative"].cummax()

            headers = ["Date", "Daily P&L", "Cumulative P&L", "Drawdown"]
            for c_idx, h in enumerate(headers):
                ws3.write(1, c_idx, h, hdr_fmt)
            for r_idx, (_, row) in enumerate(eq_df.iterrows()):
                r = r_idx + 2
                ws3.write(r, 0, str(row["date"]), text_fmt)
                ws3.write(r, 1, row["total_pnl"],   money_fmt)
                ws3.write(r, 2, row["cumulative"],  money_fmt)
                ws3.write(r, 3, row["drawdown"],    red_fmt if row["drawdown"] < 0 else money_fmt)

            # Add chart
            chart = wb.add_chart({"type": "line"})
            n_rows = len(eq_df)
            chart.add_series({
                "name":       "Cumulative P&L",
                "categories": ["Equity_Curve", 2, 0, 1 + n_rows, 0],
                "values":     ["Equity_Curve", 2, 2, 1 + n_rows, 2],
                "line":       {"color": "#2e86de", "width": 2},
            })
            chart.add_series({
                "name":       "Drawdown",
                "categories": ["Equity_Curve", 2, 0, 1 + n_rows, 0],
                "values":     ["Equity_Curve", 2, 3, 1 + n_rows, 3],
                "line":       {"color": "#e74c3c", "width": 1.5},
            })
            chart.set_title({"name": "Equity Curve & Drawdown"})
            chart.set_x_axis({"name": "Date", "num_font": {"rotation": -45}})
            chart.set_y_axis({"name": "P&L (₹)"})
            chart.set_size({"width": 800, "height": 400})
            ws3.insert_chart("F2", chart)

        # ═══════════════════════════════════════════════════════════════════
        # Sheet 4: Params Guide
        # ═══════════════════════════════════════════════════════════════════
        ws4 = wb.add_worksheet("Params_Guide")
        writer.sheets["Params_Guide"] = ws4

        guide = [
            ("Parameter",              "Description",                                          "Default"),
            ("atm_scan_start",         "Start of ATM selection window (HH:MM IST)",             "09:16"),
            ("atm_scan_end",           "End of ATM selection window (HH:MM IST)",               "09:21"),
            ("max_premium_diff",       "Max allowed |CE premium - PE premium| for ATM (₹)",     "20"),
            ("hedge_pct",              "Hedge target as % of sell premium (e.g. 0.05 = 5%)",    "0.05"),
            ("vix_intraday_threshold", "VIX intraday move % to trigger tighter SL (12-16 zone)","3.0"),
            ("atr_timeframe",          "Candle timeframe for ATR calculation",                   "5min"),
            ("atr_period",             "Lookback period for ATR",                                "14"),
            ("atr_multiplier",         "ATR multiplier for trailing SL: SL = close + N*ATR",    "1.5"),
            ("hedge_trail_step",       "Step size (₹) for hedge step-trailing after leg SL hit","3.0"),
            ("eod_exit_time",          "Square-off all positions at this time (HH:MM IST)",      "15:20"),
        ]
        ws4.write(0, 0, "Parameter Reference Guide", title_fmt)
        for r_idx, row in enumerate(guide):
            for c_idx, val in enumerate(row):
                fmt = hdr_fmt if r_idx == 0 else text_fmt
                ws4.write(r_idx + 2, c_idx, val, fmt)
        ws4.set_column(0, 0, 30)
        ws4.set_column(1, 1, 60)
        ws4.set_column(2, 2, 15)

    print(f"\n✅ Report saved: {output_path}")
    return output_path
