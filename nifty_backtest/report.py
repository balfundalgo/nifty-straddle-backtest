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

        # ═══════════════════════════════════════════════════════════════════
        # Sheet 5: Deep Analysis
        # ═══════════════════════════════════════════════════════════════════
        ws5 = wb.add_worksheet("Deep_Analysis")
        writer.sheets["Deep_Analysis"] = ws5
        _write_deep_analysis(ws5, wb, daily_results_best,
                              hdr_fmt, text_fmt, green_fmt, red_fmt,
                              money_fmt, title_fmt, int_fmt)

    print(f"\n✅ Report saved: {output_path}")
    return output_path


def _write_deep_analysis(ws, wb, results, hdr_fmt, text_fmt,
                          green_fmt, red_fmt, money_fmt, title_fmt, int_fmt):
    """Write deep analysis sheet with VIX breakdown, exit analysis, fallback stats."""
    import numpy as np
    from metrics import results_to_df

    df = results_to_df(results)
    ok = df[df["status"] == "ok"].copy()

    if ok.empty:
        ws.write(0, 0, "No traded days to analyse.")
        return

    ok["total_pnl"]    = pd.to_numeric(ok["total_pnl"],    errors="coerce").fillna(0)
    ok["vix_at_entry"] = pd.to_numeric(ok["vix_at_entry"], errors="coerce")
    ok["is_fallback"]  = ok["notes"].str.contains("FALLBACK", na=False)

    muted_fmt  = wb.add_format({"font_color": "#8b949e", "border": 1})
    gold_fmt   = wb.add_format({"bold": True, "font_color": "#e3b341",
                                 "bg_color": "#161b22"})
    sub_fmt    = wb.add_format({"bold": True, "font_color": "#58a6ff",
                                 "border": 1, "bg_color": "#0d1117"})
    pct_fmt2   = wb.add_format({"num_format": "0.0%", "border": 1})

    row = 0

    def section(title):
        nonlocal row
        ws.write(row, 0, title, gold_fmt)
        row += 1

    def write_row(label, *values, fmts=None):
        nonlocal row
        ws.write(row, 0, label, text_fmt)
        for i, v in enumerate(values):
            fmt = fmts[i] if fmts and i < len(fmts) else text_fmt
            ws.write(row, i + 1, v, fmt)
        row += 1

    def header_row(*cols):
        nonlocal row
        for i, c in enumerate(cols):
            ws.write(row, i, c, hdr_fmt)
        row += 1

    ws.set_column(0, 0, 30)
    ws.set_column(1, 8, 16)

    # ── 1. Overall summary ────────────────────────────────────────────────
    section("📊  OVERALL SUMMARY")
    header_row("Metric", "Value")
    total_days   = len(df)
    traded_days  = len(ok)
    fallback_days= ok["is_fallback"].sum()
    normal_days  = traded_days - fallback_days
    pnl_total    = float(ok["total_pnl"].sum())
    win_rate     = float((ok["total_pnl"] > 0).mean() * 100)

    for lbl, val, fmt in [
        ("Total calendar days",  total_days,    int_fmt),
        ("Traded days",          traded_days,   int_fmt),
        ("Normal ATM days",      normal_days,   int_fmt),
        ("Fallback ATM days",    fallback_days, int_fmt),
        ("Total P&L",            pnl_total,     green_fmt if pnl_total >= 0 else red_fmt),
        ("Win Rate",             win_rate / 100, pct_fmt2),
    ]:
        write_row(lbl, val, fmts=[fmt])
    row += 1

    # ── 2. Normal vs Fallback comparison ─────────────────────────────────
    section("🎯  NORMAL ATM vs FALLBACK ATM")
    header_row("Metric", "Normal ATM", "Fallback ATM")

    norm = ok[~ok["is_fallback"]]
    fall = ok[ok["is_fallback"]]

    def safe_pnl(d): return float(d["total_pnl"].sum()) if len(d) else 0.0
    def safe_wr(d):  return float((d["total_pnl"] > 0).mean()) if len(d) else 0.0
    def safe_avg(d): return float(d["total_pnl"].mean()) if len(d) else 0.0

    rows_data = [
        ("Days",        len(norm),         len(fall),         int_fmt),
        ("Total P&L",   safe_pnl(norm),    safe_pnl(fall),    money_fmt),
        ("Win Rate",    safe_wr(norm),      safe_wr(fall),     pct_fmt2),
        ("Avg P&L/day", safe_avg(norm),    safe_avg(fall),    money_fmt),
    ]
    for lbl, v1, v2, fmt in rows_data:
        ws.write(row, 0, lbl, text_fmt)
        ws.write(row, 1, v1, fmt)
        ws.write(row, 2, v2, fmt)
        row += 1
    row += 1

    # ── 3. VIX breakdown ──────────────────────────────────────────────────
    section("📈  VIX REGIME ANALYSIS")
    header_row("VIX Regime", "Days", "SL Rule", "Total P&L", "Win Rate", "Avg P&L")

    vix = ok["vix_at_entry"].dropna()
    regimes = [
        ("VIX < 12  (Low Vol)",      ok[ok["vix_at_entry"] < 12],              "40% + ₹5"),
        ("VIX 12-16 (Medium)",       ok[(ok["vix_at_entry"]>=12)&(ok["vix_at_entry"]<16)], "25-40% + ₹5"),
        ("VIX 16-20 (High)",         ok[(ok["vix_at_entry"]>=16)&(ok["vix_at_entry"]<20)], "25% + ₹5"),
        ("VIX > 20  (Very High)",    ok[ok["vix_at_entry"] >= 20],             "15% + ₹5"),
    ]
    for label, grp, sl_rule in regimes:
        if len(grp) == 0:
            ws.write(row, 0, label, text_fmt)
            ws.write(row, 1, 0, int_fmt)
            ws.write(row, 2, sl_rule, text_fmt)
            ws.write(row, 3, 0, money_fmt)
            ws.write(row, 4, "—", text_fmt)
            ws.write(row, 5, 0, money_fmt)
        else:
            pnl = float(grp["total_pnl"].sum())
            wr  = float((grp["total_pnl"] > 0).mean())
            avg = float(grp["total_pnl"].mean())
            ws.write(row, 0, label, text_fmt)
            ws.write(row, 1, len(grp), int_fmt)
            ws.write(row, 2, sl_rule, text_fmt)
            ws.write(row, 3, pnl, green_fmt if pnl >= 0 else red_fmt)
            ws.write(row, 4, wr,  pct_fmt2)
            ws.write(row, 5, avg, green_fmt if avg >= 0 else red_fmt)
        row += 1
    row += 1

    # ── 4. Exit breakdown ────────────────────────────────────────────────
    section("🚪  EXIT BREAKDOWN")
    header_row("Exit Type", "CE Count", "PE Count", "CE % of Days", "PE % of Days")

    exit_types = ["FIXED_SL", "ATR_TRAIL_SL", "EOD"]
    total = len(ok)
    for et in exit_types:
        ce_n = (ok["ce_exit_reason"] == et).sum()
        pe_n = (ok["pe_exit_reason"] == et).sum()
        ws.write(row, 0, et, text_fmt)
        ws.write(row, 1, ce_n, int_fmt)
        ws.write(row, 2, pe_n, int_fmt)
        ws.write(row, 3, ce_n / total if total else 0, pct_fmt2)
        ws.write(row, 4, pe_n / total if total else 0, pct_fmt2)
        row += 1
    row += 1

    # ── 5. Both legs SL analysis ─────────────────────────────────────────
    section("⚠️  SL HIT ANALYSIS")
    header_row("Scenario", "Days", "Total P&L", "Avg P&L")

    scenarios = [
        ("Both legs FIXED_SL",
         ok[(ok["ce_exit_reason"]=="FIXED_SL") & (ok["pe_exit_reason"]=="FIXED_SL")]),
        ("CE FIXED_SL, PE ATR_TRAIL",
         ok[(ok["ce_exit_reason"]=="FIXED_SL") & (ok["pe_exit_reason"]=="ATR_TRAIL_SL")]),
        ("PE FIXED_SL, CE ATR_TRAIL",
         ok[(ok["pe_exit_reason"]=="FIXED_SL") & (ok["ce_exit_reason"]=="ATR_TRAIL_SL")]),
        ("Both legs EOD (no SL)",
         ok[(ok["ce_exit_reason"]=="EOD") & (ok["pe_exit_reason"]=="EOD")]),
        ("One leg EOD, one FIXED_SL",
         ok[((ok["ce_exit_reason"]=="EOD")&(ok["pe_exit_reason"]=="FIXED_SL")) |
            ((ok["pe_exit_reason"]=="EOD")&(ok["ce_exit_reason"]=="FIXED_SL"))]),
    ]
    for label, grp in scenarios:
        pnl = float(grp["total_pnl"].sum()) if len(grp) else 0
        avg = float(grp["total_pnl"].mean()) if len(grp) else 0
        ws.write(row, 0, label, text_fmt)
        ws.write(row, 1, len(grp), int_fmt)
        ws.write(row, 2, pnl, green_fmt if pnl >= 0 else red_fmt)
        ws.write(row, 3, avg, green_fmt if avg >= 0 else red_fmt)
        row += 1
    row += 1

    # ── 6. Hedge performance ─────────────────────────────────────────────
    section("🛡️  HEDGE PERFORMANCE")
    header_row("Metric", "CE Hedge", "PE Hedge")

    ok["ce_hedge_pnl"] = pd.to_numeric(ok.get("ce_hedge_pnl", 0), errors="coerce").fillna(0)
    ok["pe_hedge_pnl"] = pd.to_numeric(ok.get("pe_hedge_pnl", 0), errors="coerce").fillna(0)

    ce_step = (ok["ce_hedge_exit_reason"] == "STEP_TRAIL_SL").sum()
    pe_step = (ok["pe_hedge_exit_reason"] == "STEP_TRAIL_SL").sum()
    ce_eod  = (ok["ce_hedge_exit_reason"] == "EOD").sum()
    pe_eod  = (ok["pe_hedge_exit_reason"] == "EOD").sum()
    ce_miss = ok["ce_hedge_exit"].isna().sum()
    pe_miss = ok["pe_hedge_exit"].isna().sum()

    hedge_rows = [
        ("Step Trail SL exits",   ce_step, pe_step, int_fmt),
        ("EOD exits",             ce_eod,  pe_eod,  int_fmt),
        ("Missing exits",         ce_miss, pe_miss, int_fmt),
        ("Total Hedge P&L",
         float(ok["ce_hedge_pnl"].sum()),
         float(ok["pe_hedge_pnl"].sum()), money_fmt),
    ]
    for lbl, v1, v2, fmt in hedge_rows:
        ws.write(row, 0, lbl, text_fmt)
        ws.write(row, 1, v1, fmt)
        ws.write(row, 2, v2, fmt)
        row += 1
    row += 1

    # ── 7. Monthly breakdown ─────────────────────────────────────────────
    section("📅  MONTHLY BREAKDOWN")
    header_row("Month", "Days", "Total P&L", "Win Rate", "Avg P&L", "Best Day", "Worst Day")

    ok["month"] = pd.to_datetime(ok["date"]).dt.strftime("%Y-%m")
    for month, grp in ok.groupby("month"):
        pnl  = float(grp["total_pnl"].sum())
        wr   = float((grp["total_pnl"] > 0).mean())
        avg  = float(grp["total_pnl"].mean())
        best = float(grp["total_pnl"].max())
        wrst = float(grp["total_pnl"].min())
        ws.write(row, 0, month, text_fmt)
        ws.write(row, 1, len(grp), int_fmt)
        ws.write(row, 2, pnl,  green_fmt if pnl  >= 0 else red_fmt)
        ws.write(row, 3, wr,   pct_fmt2)
        ws.write(row, 4, avg,  green_fmt if avg  >= 0 else red_fmt)
        ws.write(row, 5, best, green_fmt)
        ws.write(row, 6, wrst, red_fmt)
        row += 1

    ws.freeze_panes(0, 1)
