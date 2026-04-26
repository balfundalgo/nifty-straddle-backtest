"""
metrics.py — Compute performance metrics from a list of DayResult objects.
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Any
from day_simulator import DayResult


def compute_metrics(results: List[DayResult], params_dict: dict = None) -> Dict[str, Any]:
    """
    Given a list of DayResult objects, compute all performance metrics.
    Returns a flat dict suitable for one row in the results DataFrame.
    """
    # Filter tradeable days only
    tradeable = [r for r in results if r.status == "ok"]
    total_days  = len(results)
    traded_days = len(tradeable)
    skipped_days = total_days - traded_days

    if traded_days == 0:
        base = {"traded_days": 0, "total_pnl": 0.0, "status": "no_trades"}
        if params_dict:
            base.update(params_dict)
        return base

    pnls = np.array([r.total_pnl for r in tradeable])

    # ── Basic ──────────────────────────────────────────────────────────────
    total_pnl    = float(pnls.sum())
    avg_daily    = float(pnls.mean())
    std_daily    = float(pnls.std())
    win_rate     = float((pnls > 0).mean())
    winners      = pnls[pnls > 0]
    losers       = pnls[pnls < 0]
    avg_win      = float(winners.mean()) if len(winners) else 0.0
    avg_loss     = float(losers.mean())  if len(losers)  else 0.0
    max_win      = float(winners.max())  if len(winners) else 0.0
    max_loss     = float(losers.min())   if len(losers)  else 0.0
    profit_factor = (winners.sum() / abs(losers.sum())) if len(losers) and losers.sum() != 0 else float("inf")

    # ── Sharpe (annualized, 252 trading days) ──────────────────────────────
    sharpe = (avg_daily / std_daily * np.sqrt(252)) if std_daily > 0 else 0.0

    # ── Standard Deviation of daily P&L ─────────────────────────────────
    # std_daily already computed above (daily P&L std dev)
    # Also compute annualized std dev
    std_annual = float(std_daily * np.sqrt(252))

    # ── Max Drawdown ───────────────────────────────────────────────────────
    cumulative  = np.cumsum(pnls)
    running_max = np.maximum.accumulate(cumulative)
    drawdowns   = cumulative - running_max
    max_dd      = float(drawdowns.min())
    # Max drawdown % relative to peak
    non_zero_peaks = running_max[running_max > 0]
    dd_pcts     = drawdowns[running_max > 0] / running_max[running_max > 0] * 100
    max_dd_pct  = float(dd_pcts.min()) if len(dd_pcts) else 0.0

    # ── Consecutive metrics ────────────────────────────────────────────────
    max_consec_wins   = _max_consecutive(pnls > 0)
    max_consec_losses = _max_consecutive(pnls < 0)

    # ── SL hit analysis ───────────────────────────────────────────────────
    both_sl    = sum(1 for r in tradeable if "SL" in r.ce_exit_reason and "SL" in r.pe_exit_reason)
    one_sl     = sum(1 for r in tradeable if
                     ("SL" in r.ce_exit_reason) != ("SL" in r.pe_exit_reason))
    eod_exits  = sum(1 for r in tradeable if "EOD" in r.ce_exit_reason or "EOD" in r.pe_exit_reason)

    # ── Recovery ratio ────────────────────────────────────────────────────
    recovery_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else float("inf")

    out = {
        # Summary
        "total_pnl":         round(total_pnl, 2),
        "traded_days":       traded_days,
        "skipped_days":      skipped_days,
        "win_rate_pct":      round(win_rate * 100, 1),

        # Per-trade
        "avg_daily_pnl":     round(avg_daily, 2),
        "std_daily_pnl":     round(std_daily, 2),
        "std_annual_pnl":    round(std_annual, 2),
        "avg_win":           round(avg_win,   2),
        "avg_loss":          round(avg_loss,  2),
        "max_win":           round(max_win,   2),
        "max_loss":          round(max_loss,  2),
        "profit_factor":     round(profit_factor, 3),
        "recovery_ratio":    round(recovery_ratio, 3),

        # Risk
        "sharpe":            round(sharpe, 3),
        "max_drawdown":      round(max_dd,     2),
        "max_drawdown_pct":  round(max_dd_pct, 2),

        # Streaks
        "max_consec_wins":   max_consec_wins,
        "max_consec_losses": max_consec_losses,

        # SL stats
        "both_legs_sl":      both_sl,
        "one_leg_sl":        one_sl,
        "eod_exits":         eod_exits,
    }

    # Append params if provided
    if params_dict:
        out.update(params_dict)

    return out


def _max_consecutive(bool_array: np.ndarray) -> int:
    """Count maximum consecutive True values in a boolean array."""
    max_streak = cur_streak = 0
    for val in bool_array:
        if val:
            cur_streak += 1
            max_streak = max(max_streak, cur_streak)
        else:
            cur_streak = 0
    return max_streak


def results_to_df(results: List[DayResult]) -> pd.DataFrame:
    """Convert list of DayResult to a DataFrame (one row per day)."""
    return pd.DataFrame([r.to_dict() for r in results])


def rank_param_sets(metrics_list: List[Dict]) -> pd.DataFrame:
    """
    Given a list of metrics dicts (one per param combination),
    return a DataFrame sorted by total_pnl descending.
    Adds a rank column.
    """
    df = pd.DataFrame(metrics_list)
    df = df.sort_values("total_pnl", ascending=False).reset_index(drop=True)
    df.insert(0, "rank", df.index + 1)
    return df
