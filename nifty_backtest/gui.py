"""
gui.py — Balfund NIFTY Straddle Backtest — Desktop GUI
Drop this file into the same folder as strategy.py, config.py, etc.
Run:  python gui.py
"""

import sys
import os
import threading
import queue
import subprocess
import json
from datetime import date, datetime
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox

import customtkinter as ctk

# ─── Balfund Brand Colors ────────────────────────────────────────────────────
NAVY      = "#0D1B2A"
NAVY_MID  = "#162032"
NAVY_CARD = "#1A2740"
GOLD      = "#C9A84C"
GOLD_DARK = "#A0802A"
TEXT      = "#E8EAF0"
TEXT_DIM  = "#8A9BB5"
GREEN     = "#2ECC71"
RED       = "#E74C3C"
ORANGE    = "#F39C12"
ACCENT    = "#3A7BD5"
BORDER    = "#2A3F5F"

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

# ─── Font helpers ─────────────────────────────────────────────────────────────
def F(size, weight="normal"): return ctk.CTkFont(family="Consolas", size=size, weight=weight)
def FU(size, weight="normal"): return ctk.CTkFont(size=size, weight=weight)

# ─── Reusable Widget Factory ──────────────────────────────────────────────────

def section_label(parent, text):
    return ctk.CTkLabel(parent, text=text, font=FU(11, "bold"),
                        text_color=GOLD, anchor="w")

def param_label(parent, text):
    return ctk.CTkLabel(parent, text=text, font=FU(12),
                        text_color=TEXT_DIM, anchor="w")

def make_entry(parent, width=120, placeholder="", initial=""):
    e = ctk.CTkEntry(parent, width=width, font=F(12),
                     fg_color=NAVY, border_color=BORDER,
                     text_color=TEXT, placeholder_text=placeholder,
                     corner_radius=6)
    if initial:
        e.insert(0, initial)
    return e

def make_optionmenu(parent, values, initial, width=120):
    var = ctk.StringVar(value=initial)
    om = ctk.CTkOptionMenu(parent, values=values, variable=var,
                           width=width, font=FU(12),
                           fg_color=NAVY_CARD, button_color=ACCENT,
                           button_hover_color=GOLD_DARK,
                           text_color=TEXT, corner_radius=6)
    return om, var

def card(parent, **kwargs):
    return ctk.CTkFrame(parent, fg_color=NAVY_CARD, corner_radius=10,
                        border_width=1, border_color=BORDER, **kwargs)

def row_frame(parent):
    return ctk.CTkFrame(parent, fg_color="transparent")

def divider(parent):
    return ctk.CTkFrame(parent, height=1, fg_color=BORDER)


# ═══════════════════════════════════════════════════════════════════════════════
#  PARAMETER PANEL — Single Run
# ═══════════════════════════════════════════════════════════════════════════════

class SingleRunPanel(ctk.CTkScrollableFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self._build()

    def _build(self):
        p = self

        # ── ATM Selection ─────────────────────────────────────────────────
        section_label(p, "📡  ATM SELECTION").pack(anchor="w", pady=(10, 4), padx=4)
        c = card(p); c.pack(fill="x", pady=(0, 10), padx=2)
        g = ctk.CTkFrame(c, fg_color="transparent"); g.pack(fill="x", padx=14, pady=12)

        self.atm_start = self._row(g, "ATM Scan Start", "HH:MM", "09:16", 0)
        self.atm_end   = self._row(g, "ATM Scan End",   "HH:MM", "09:21", 1)
        self.max_diff  = self._row(g, "Max Premium Diff (₹)", "e.g. 20", "20.0", 2)

        # ── Hedge ────────────────────────────────────────────────────────
        section_label(p, "🛡️  HEDGE").pack(anchor="w", pady=(4, 4), padx=4)
        c2 = card(p); c2.pack(fill="x", pady=(0, 10), padx=2)
        g2 = ctk.CTkFrame(c2, fg_color="transparent"); g2.pack(fill="x", padx=14, pady=12)
        self.hedge_pct    = self._row(g2, "Hedge % of Premium", "e.g. 0.05", "0.05", 0)
        self.trail_step   = self._row(g2, "Hedge Trail Step (₹)", "e.g. 3.0", "3.0", 1)

        # ── VIX ──────────────────────────────────────────────────────────
        section_label(p, "📊  VIX REGIMES").pack(anchor="w", pady=(4, 4), padx=4)
        c3 = card(p); c3.pack(fill="x", pady=(0, 10), padx=2)
        g3 = ctk.CTkFrame(c3, fg_color="transparent"); g3.pack(fill="x", padx=14, pady=12)
        self.vix_low      = self._row(g3, "VIX Low Threshold",      "e.g. 12", "12.0", 0)
        self.vix_mid_low  = self._row(g3, "VIX Mid-Low Threshold",  "e.g. 16", "16.0", 1)
        self.vix_mid_high = self._row(g3, "VIX Mid-High Threshold", "e.g. 20", "20.0", 2)
        self.vix_intra    = self._row(g3, "VIX Intraday Trigger %", "e.g. 3.0", "3.0",  3)

        # ── Stop Loss ────────────────────────────────────────────────────
        section_label(p, "🛑  STOP LOSS").pack(anchor="w", pady=(4, 4), padx=4)
        c4 = card(p); c4.pack(fill="x", pady=(0, 10), padx=2)
        g4 = ctk.CTkFrame(c4, fg_color="transparent"); g4.pack(fill="x", padx=14, pady=12)
        self.sl_lt12      = self._row(g4, "SL % — VIX < 12",           "e.g. 0.40", "0.40", 0)
        self.sl_1216_calm = self._row(g4, "SL % — VIX 12-16 Calm",     "e.g. 0.40", "0.40", 1)
        self.sl_1216_vol  = self._row(g4, "SL % — VIX 12-16 Volatile", "e.g. 0.25", "0.25", 2)
        self.sl_1620      = self._row(g4, "SL % — VIX 16-20",          "e.g. 0.25", "0.25", 3)
        self.sl_gt20      = self._row(g4, "SL % — VIX > 20",           "e.g. 0.15", "0.15", 4)
        self.sl_buffer    = self._row(g4, "SL Flat Buffer (₹)",         "e.g. 5.0",  "5.0",  5)

        # ── ATR Trailing ─────────────────────────────────────────────────
        section_label(p, "📉  ATR TRAILING (surviving leg)").pack(anchor="w", pady=(4, 4), padx=4)
        c5 = card(p); c5.pack(fill="x", pady=(0, 10), padx=2)
        g5 = ctk.CTkFrame(c5, fg_color="transparent"); g5.pack(fill="x", padx=14, pady=12)

        # ATR timeframe is a dropdown
        param_label(g5, "ATR Timeframe").grid(row=0, column=0, sticky="w", padx=(0,16), pady=4)
        self.atr_tf_menu, self.atr_tf_var = make_optionmenu(
            g5, ["1min", "5min", "15min"], "5min", width=130)
        self.atr_tf_menu.grid(row=0, column=1, sticky="w", pady=4)

        self.atr_period = self._row(g5, "ATR Period",     "e.g. 14",  "14",  1)
        self.atr_mult   = self._row(g5, "ATR Multiplier", "e.g. 1.5", "1.5", 2)

        # ── Exit & Lot ───────────────────────────────────────────────────
        section_label(p, "⏰  EXIT & POSITION").pack(anchor="w", pady=(4, 4), padx=4)
        c6 = card(p); c6.pack(fill="x", pady=(0, 10), padx=2)
        g6 = ctk.CTkFrame(c6, fg_color="transparent"); g6.pack(fill="x", padx=14, pady=12)
        self.eod_time = self._row(g6, "EOD Exit Time", "HH:MM",   "15:20", 0)
        self.lot_size = self._row(g6, "Lot Size",      "e.g. 75", "75",    1)

    def _row(self, parent, label, placeholder, initial, row):
        param_label(parent, label).grid(row=row, column=0, sticky="w",
                                        padx=(0, 16), pady=4)
        e = make_entry(parent, width=130, placeholder=placeholder, initial=initial)
        e.grid(row=row, column=1, sticky="w", pady=4)
        return e

    def get_params(self) -> dict:
        return {
            "atm_scan_start":          self.atm_start.get().strip(),
            "atm_scan_end":            self.atm_end.get().strip(),
            "max_premium_diff":        float(self.max_diff.get()),
            "hedge_pct":               float(self.hedge_pct.get()),
            "hedge_trail_step":        float(self.trail_step.get()),
            "vix_low":                 float(self.vix_low.get()),
            "vix_mid_low":             float(self.vix_mid_low.get()),
            "vix_mid_high":            float(self.vix_mid_high.get()),
            "vix_intraday_threshold":  float(self.vix_intra.get()),
            "sl_pct_vix_lt12":         float(self.sl_lt12.get()),
            "sl_pct_vix_12_16_calm":   float(self.sl_1216_calm.get()),
            "sl_pct_vix_12_16_volatile": float(self.sl_1216_vol.get()),
            "sl_pct_vix_16_20":        float(self.sl_1620.get()),
            "sl_pct_vix_gt20":         float(self.sl_gt20.get()),
            "sl_buffer":               float(self.sl_buffer.get()),
            "atr_timeframe":           self.atr_tf_var.get(),
            "atr_period":              int(self.atr_period.get()),
            "atr_multiplier":          float(self.atr_mult.get()),
            "eod_exit_time":           self.eod_time.get().strip(),
            "lot_size":                int(self.lot_size.get()),
        }


# ═══════════════════════════════════════════════════════════════════════════════
#  PARAMETER PANEL — Grid Search
# ═══════════════════════════════════════════════════════════════════════════════

class GridSearchPanel(ctk.CTkScrollableFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, fg_color="transparent", **kwargs)
        self._build()

    def _build(self):
        p = self
        hint = ctk.CTkLabel(p, text="Enter comma-separated values for each parameter",
                            font=FU(11), text_color=TEXT_DIM)
        hint.pack(anchor="w", pady=(6, 10), padx=4)

        # ── ATM ──────────────────────────────────────────────────────────
        section_label(p, "📡  ATM SELECTION").pack(anchor="w", pady=(0, 4), padx=4)
        c = card(p); c.pack(fill="x", pady=(0, 10), padx=2)
        g = ctk.CTkFrame(c, fg_color="transparent"); g.pack(fill="x", padx=14, pady=12)
        self.g_atm_starts = self._row(g, "ATM Scan Starts",    "09:16, 09:17",       "09:16, 09:17", 0)
        self.g_atm_ends   = self._row(g, "ATM Scan Ends",      "09:20, 09:21",       "09:20, 09:21", 1)
        self.g_max_diffs  = self._row(g, "Max Premium Diffs",  "10, 20, 30",         "10.0, 20.0, 30.0", 2)

        # ── Hedge ────────────────────────────────────────────────────────
        section_label(p, "🛡️  HEDGE").pack(anchor="w", pady=(4, 4), padx=4)
        c2 = card(p); c2.pack(fill="x", pady=(0, 10), padx=2)
        g2 = ctk.CTkFrame(c2, fg_color="transparent"); g2.pack(fill="x", padx=14, pady=12)
        self.g_hedge_pcts  = self._row(g2, "Hedge Pct Values",    "0.03, 0.05, 0.07", "0.03, 0.05, 0.07", 0)
        self.g_trail_steps = self._row(g2, "Trail Step Values",   "2.0, 3.0, 4.0",    "2.0, 3.0, 4.0", 1)

        # ── VIX ──────────────────────────────────────────────────────────
        section_label(p, "📊  VIX INTRADAY THRESHOLD").pack(anchor="w", pady=(4, 4), padx=4)
        c3 = card(p); c3.pack(fill="x", pady=(0, 10), padx=2)
        g3 = ctk.CTkFrame(c3, fg_color="transparent"); g3.pack(fill="x", padx=14, pady=12)
        self.g_vix_thresholds = self._row(g3, "VIX Intraday % Triggers", "2.0, 3.0, 4.0", "2.0, 3.0, 4.0", 0)

        # ── ATR ──────────────────────────────────────────────────────────
        section_label(p, "📉  ATR TRAILING").pack(anchor="w", pady=(4, 4), padx=4)
        c4 = card(p); c4.pack(fill="x", pady=(0, 10), padx=2)
        g4 = ctk.CTkFrame(c4, fg_color="transparent"); g4.pack(fill="x", padx=14, pady=12)
        self.g_atr_tfs     = self._row(g4, "ATR Timeframes", "1min, 5min, 15min", "1min, 5min, 15min", 0)
        self.g_atr_periods = self._row(g4, "ATR Periods",    "7, 14, 21",         "7, 14, 21", 1)
        self.g_atr_mults   = self._row(g4, "ATR Multipliers","1.0, 1.5, 2.0",     "1.0, 1.5, 2.0", 2)

        # ── EOD ──────────────────────────────────────────────────────────
        section_label(p, "⏰  EOD EXIT TIMES").pack(anchor="w", pady=(4, 4), padx=4)
        c5 = card(p); c5.pack(fill="x", pady=(0, 10), padx=2)
        g5 = ctk.CTkFrame(c5, fg_color="transparent"); g5.pack(fill="x", padx=14, pady=12)
        self.g_eod_times = self._row(g5, "EOD Exit Times", "15:15, 15:20, 15:25", "15:15, 15:20, 15:25", 0)

        # ── Fast mode toggle ──────────────────────────────────────────────
        section_label(p, "⚡  FAST MODE").pack(anchor="w", pady=(4, 4), padx=4)
        c6 = card(p); c6.pack(fill="x", pady=(0, 10), padx=2)
        gf = ctk.CTkFrame(c6, fg_color="transparent"); gf.pack(fill="x", padx=14, pady=12)
        self.fast_mode_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(gf, text="Fast Mode  (reduces ATR timeframes/periods/multipliers)",
                        variable=self.fast_mode_var, font=FU(12),
                        text_color=TEXT, checkmark_color=NAVY,
                        fg_color=GOLD, hover_color=GOLD_DARK,
                        border_color=BORDER).grid(row=0, column=0, sticky="w")

        # ── Combo count display ───────────────────────────────────────────
        self.combo_lbl = ctk.CTkLabel(p, text="", font=FU(12, "bold"), text_color=ORANGE)
        self.combo_lbl.pack(anchor="w", padx=4, pady=(0, 8))
        self._bind_combo_update()

    def _row(self, parent, label, placeholder, initial, row):
        param_label(parent, label).grid(row=row, column=0, sticky="w",
                                        padx=(0, 16), pady=4)
        e = make_entry(parent, width=260, placeholder=placeholder, initial=initial)
        e.grid(row=row, column=1, sticky="w", pady=4)
        return e

    def _bind_combo_update(self):
        widgets = [self.g_atm_starts, self.g_atm_ends, self.g_max_diffs,
                   self.g_hedge_pcts, self.g_trail_steps, self.g_vix_thresholds,
                   self.g_atr_tfs, self.g_atr_periods, self.g_atr_mults, self.g_eod_times]
        for w in widgets:
            w.bind("<KeyRelease>", lambda e: self._update_combo_count())

    def _parse_list(self, widget):
        raw = widget.get().strip()
        if not raw: return []
        return [x.strip() for x in raw.split(",") if x.strip()]

    def _update_combo_count(self):
        try:
            total = 1
            for w in [self.g_atm_starts, self.g_atm_ends, self.g_max_diffs,
                      self.g_hedge_pcts, self.g_trail_steps, self.g_vix_thresholds,
                      self.g_atr_tfs, self.g_atr_periods, self.g_atr_mults, self.g_eod_times]:
                n = len(self._parse_list(w))
                if n: total *= n
            self.combo_lbl.configure(text=f"⚡  Total combinations: {total:,}")
        except:
            pass

    def get_grid_config(self) -> dict:
        def floats(w): return [float(x) for x in self._parse_list(w)]
        def ints(w):   return [int(x)   for x in self._parse_list(w)]
        def strs(w):   return self._parse_list(w)
        return {
            "atm_scan_starts":         strs(self.g_atm_starts),
            "atm_scan_ends":           strs(self.g_atm_ends),
            "max_premium_diffs":       floats(self.g_max_diffs),
            "hedge_pcts":              floats(self.g_hedge_pcts),
            "hedge_trail_steps":       floats(self.g_trail_steps),
            "vix_intraday_thresholds": floats(self.g_vix_thresholds),
            "atr_timeframes":          strs(self.g_atr_tfs),
            "atr_periods":             ints(self.g_atr_periods),
            "atr_multipliers":         floats(self.g_atr_mults),
            "eod_exit_times":          strs(self.g_eod_times),
            "fast_mode":               self.fast_mode_var.get(),
        }


# ═══════════════════════════════════════════════════════════════════════════════
#  RESULTS TABLE
# ═══════════════════════════════════════════════════════════════════════════════

class ResultsTable(ctk.CTkFrame):
    """Simple scrollable table to show metrics after a run."""
    COLS = [
        ("Metric", 220),
        ("Value",  200),
    ]

    def __init__(self, master, **kwargs):
        super().__init__(master, fg_color=NAVY_CARD, corner_radius=10,
                         border_width=1, border_color=BORDER, **kwargs)
        self._rows = []
        self._build()

    def _build(self):
        hdr = ctk.CTkFrame(self, fg_color=BORDER, corner_radius=0)
        hdr.pack(fill="x", padx=1, pady=(1, 0))
        for col, w in self.COLS:
            ctk.CTkLabel(hdr, text=col, font=FU(11, "bold"),
                         text_color=GOLD, width=w, anchor="w").pack(
                side="left", padx=8, pady=6)

        self.scroll_frame = ctk.CTkScrollableFrame(self, fg_color="transparent",
                                                    corner_radius=0)
        self.scroll_frame.pack(fill="both", expand=True, padx=0, pady=0)

    def clear(self):
        for w in self.scroll_frame.winfo_children():
            w.destroy()

    def populate(self, metrics: dict):
        self.clear()
        display_order = [
            ("total_pnl",          "Total P&L (₹)"),
            ("traded_days",        "Traded Days"),
            ("skipped_days",       "Skipped Days"),
            ("win_rate_pct",       "Win Rate (%)"),
            ("avg_daily_pnl",      "Avg Daily P&L (₹)"),
            ("std_daily_pnl",      "Std Dev Daily P&L"),
            ("avg_win",            "Avg Win (₹)"),
            ("avg_loss",           "Avg Loss (₹)"),
            ("max_win",            "Max Win (₹)"),
            ("max_loss",           "Max Loss (₹)"),
            ("profit_factor",      "Profit Factor"),
            ("recovery_ratio",     "Recovery Ratio"),
            ("sharpe",             "Sharpe Ratio (Annual)"),
            ("max_drawdown",       "Max Drawdown (₹)"),
            ("max_drawdown_pct",   "Max Drawdown (%)"),
            ("max_consec_wins",    "Max Consecutive Wins"),
            ("max_consec_losses",  "Max Consecutive Losses"),
            ("both_legs_sl",       "Both Legs SL Days"),
            ("one_leg_sl",         "One Leg SL Days"),
            ("eod_exits",          "EOD Exits"),
        ]
        for i, (key, label) in enumerate(display_order):
            val = metrics.get(key, "—")
            bg = NAVY if i % 2 == 0 else NAVY_CARD
            row = ctk.CTkFrame(self.scroll_frame, fg_color=bg, corner_radius=0)
            row.pack(fill="x")

            # color-code pnl
            val_color = TEXT
            if key == "total_pnl" and isinstance(val, (int, float)):
                val_color = GREEN if val >= 0 else RED
            if key == "win_rate_pct" and isinstance(val, (int, float)):
                val_color = GREEN if val >= 50 else ORANGE

            ctk.CTkLabel(row, text=label, font=FU(12), text_color=TEXT_DIM,
                         width=220, anchor="w").pack(side="left", padx=8, pady=5)
            fmt = f"₹{val:>,.2f}" if "pnl" in key or key in ("avg_win","avg_loss","max_win","max_loss","max_drawdown") else (
                  f"{val:.3f}"    if isinstance(val, float) else str(val))
            ctk.CTkLabel(row, text=fmt, font=F(12, "bold"), text_color=val_color,
                         width=200, anchor="w").pack(side="left", padx=8, pady=5)


# ═══════════════════════════════════════════════════════════════════════════════
#  LOG PANEL
# ═══════════════════════════════════════════════════════════════════════════════

class LogPanel(ctk.CTkFrame):
    def __init__(self, master, **kwargs):
        super().__init__(master, fg_color=NAVY, corner_radius=8,
                         border_width=1, border_color=BORDER, **kwargs)
        hdr = ctk.CTkFrame(self, fg_color=NAVY_CARD, corner_radius=0)
        hdr.pack(fill="x")
        ctk.CTkLabel(hdr, text="  📋 Console Output", font=FU(11, "bold"),
                     text_color=GOLD).pack(side="left", pady=5)
        ctk.CTkButton(hdr, text="Clear", width=60, height=26,
                      font=FU(10), fg_color=BORDER, hover_color=NAVY_MID,
                      text_color=TEXT_DIM, corner_radius=4,
                      command=self.clear).pack(side="right", padx=8, pady=4)

        self.textbox = ctk.CTkTextbox(self, font=F(11), fg_color=NAVY,
                                       text_color="#A8C8A0",
                                       corner_radius=0, wrap="word")
        self.textbox.pack(fill="both", expand=True, padx=2, pady=2)
        self.textbox.configure(state="disabled")

    def append(self, text: str, color: str = None):
        self.textbox.configure(state="normal")
        self.textbox.insert("end", text)
        self.textbox.see("end")
        self.textbox.configure(state="disabled")

    def clear(self):
        self.textbox.configure(state="normal")
        self.textbox.delete("1.0", "end")
        self.textbox.configure(state="disabled")


# ═══════════════════════════════════════════════════════════════════════════════
#  MAIN APP WINDOW
# ═══════════════════════════════════════════════════════════════════════════════

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Balfund — NIFTY Straddle Backtest")
        self.geometry("1280x820")
        self.minsize(1050, 700)
        self.configure(fg_color=NAVY)

        self._running = False
        self._proc    = None
        self._q       = queue.Queue()

        self._build_ui()
        self.after(100, self._poll_queue)

    # ─── Layout ──────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Header ──────────────────────────────────────────────────────
        hdr = ctk.CTkFrame(self, fg_color=NAVY_CARD, height=58,
                           corner_radius=0, border_width=0)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        ctk.CTkLabel(hdr, text="⬡  BALFUND", font=FU(20, "bold"),
                     text_color=GOLD).pack(side="left", padx=20)
        ctk.CTkLabel(hdr, text="NIFTY Straddle Backtest  v2.0",
                     font=FU(13), text_color=TEXT_DIM).pack(side="left", padx=2)

        self.status_lbl = ctk.CTkLabel(hdr, text="● Idle", font=FU(12, "bold"),
                                        text_color=TEXT_DIM)
        self.status_lbl.pack(side="right", padx=20)

        # ── Body (sidebar + content) ──────────────────────────────────────
        body = ctk.CTkFrame(self, fg_color="transparent")
        body.pack(fill="both", expand=True)

        # Sidebar
        self._build_sidebar(body)

        # Right content area (top: params, bottom: log)
        right = ctk.CTkFrame(body, fg_color="transparent")
        right.pack(side="left", fill="both", expand=True, padx=(0, 8), pady=8)

        # Top strip: data path + date range + run button
        self._build_top_strip(right)

        # Tab content area
        self.content_area = ctk.CTkFrame(right, fg_color="transparent")
        self.content_area.pack(fill="both", expand=True)

        # Build pages
        self.pages = {}
        self._build_single_page()
        self._build_grid_page()
        self._build_results_page()

        # Log panel (always visible at bottom)
        self.log = LogPanel(right)
        self.log.pack(fill="x", pady=(4, 0), ipady=2)
        self.log.configure(height=200)

        # Show default page
        self._show_page("single")

    def _build_sidebar(self, parent):
        sb = ctk.CTkFrame(parent, fg_color=NAVY_CARD, width=170,
                          corner_radius=0, border_width=0)
        sb.pack(side="left", fill="y")
        sb.pack_propagate(False)

        ctk.CTkLabel(sb, text="MODE", font=FU(10, "bold"),
                     text_color=TEXT_DIM).pack(pady=(20, 8), padx=12, anchor="w")

        self._nav_btns = {}
        for key, icon, label in [
            ("single",  "▶", "Single Run"),
            ("grid",    "⊞", "Grid Search"),
            ("results", "📊", "Results"),
        ]:
            btn = ctk.CTkButton(sb, text=f" {icon}  {label}", anchor="w",
                                font=FU(13), height=42, corner_radius=8,
                                fg_color="transparent", hover_color=BORDER,
                                text_color=TEXT_DIM, border_width=0,
                                command=lambda k=key: self._show_page(k))
            btn.pack(fill="x", padx=8, pady=2)
            self._nav_btns[key] = btn

        divider(sb).pack(fill="x", padx=12, pady=14)

        # Quick stats in sidebar
        self.sb_pnl  = self._sb_stat(sb, "Last P&L", "—")
        self.sb_wr   = self._sb_stat(sb, "Win Rate",  "—")
        self.sb_sh   = self._sb_stat(sb, "Sharpe",    "—")
        self.sb_dd   = self._sb_stat(sb, "Max DD",    "—")

    def _sb_stat(self, parent, label, initial):
        f = ctk.CTkFrame(parent, fg_color="transparent")
        f.pack(fill="x", padx=14, pady=3)
        ctk.CTkLabel(f, text=label, font=FU(10), text_color=TEXT_DIM,
                     anchor="w").pack(fill="x")
        lbl = ctk.CTkLabel(f, text=initial, font=FU(13, "bold"),
                           text_color=GOLD, anchor="w")
        lbl.pack(fill="x")
        return lbl

    def _build_top_strip(self, parent):
        strip = ctk.CTkFrame(parent, fg_color=NAVY_CARD, corner_radius=10,
                             border_width=1, border_color=BORDER)
        strip.pack(fill="x", pady=(0, 6))

        # Row 1: data path
        r1 = ctk.CTkFrame(strip, fg_color="transparent")
        r1.pack(fill="x", padx=12, pady=(10, 4))
        param_label(r1, "Data Path:").pack(side="left", padx=(0, 8))
        self.data_path_var = ctk.StringVar(
            value=r"C:\Users\Admin\Downloads\BreezeDownloader-v1.4.2\breeze_data")
        self.data_path_entry = ctk.CTkEntry(r1, textvariable=self.data_path_var,
                                            width=480, font=F(11),
                                            fg_color=NAVY, border_color=BORDER,
                                            text_color=TEXT, corner_radius=6)
        self.data_path_entry.pack(side="left")
        ctk.CTkButton(r1, text="Browse…", width=80, height=30, font=FU(11),
                      fg_color=BORDER, hover_color=NAVY_MID, text_color=TEXT,
                      corner_radius=6, command=self._browse_path).pack(side="left", padx=8)

        # Row 2: dates + run
        r2 = ctk.CTkFrame(strip, fg_color="transparent")
        r2.pack(fill="x", padx=12, pady=(0, 10))

        param_label(r2, "From:").pack(side="left", padx=(0, 6))
        self.from_date = make_entry(r2, width=110, placeholder="YYYY-MM-DD",
                                    initial="2024-01-01")
        self.from_date.pack(side="left", padx=(0, 14))

        param_label(r2, "To:").pack(side="left", padx=(0, 6))
        self.to_date = make_entry(r2, width=110, placeholder="YYYY-MM-DD",
                                  initial=date.today().strftime("%Y-%m-%d"))
        self.to_date.pack(side="left", padx=(0, 20))

        # Run / Stop buttons
        self.run_btn = ctk.CTkButton(r2, text="▶  RUN BACKTEST", width=180,
                                      height=36, font=FU(13, "bold"),
                                      fg_color=GOLD, hover_color=GOLD_DARK,
                                      text_color=NAVY, corner_radius=8,
                                      command=self._on_run)
        self.run_btn.pack(side="left", padx=(0, 8))

        self.stop_btn = ctk.CTkButton(r2, text="⏹ STOP", width=90,
                                       height=36, font=FU(12, "bold"),
                                       fg_color="#8B2020", hover_color="#6B1010",
                                       text_color=TEXT, corner_radius=8,
                                       command=self._on_stop, state="disabled")
        self.stop_btn.pack(side="left")

    def _build_single_page(self):
        p = ctk.CTkFrame(self.content_area, fg_color="transparent")
        self.single_panel = SingleRunPanel(p)
        self.single_panel.pack(fill="both", expand=True)
        self.pages["single"] = p

    def _build_grid_page(self):
        p = ctk.CTkFrame(self.content_area, fg_color="transparent")
        self.grid_panel = GridSearchPanel(p)
        self.grid_panel.pack(fill="both", expand=True)
        self.pages["grid"] = p

    def _build_results_page(self):
        p = ctk.CTkFrame(self.content_area, fg_color="transparent")
        ctk.CTkLabel(p, text="Performance Metrics — Last Run",
                     font=FU(13, "bold"), text_color=GOLD).pack(
                         anchor="w", padx=4, pady=(8, 6))
        self.results_table = ResultsTable(p)
        self.results_table.pack(fill="both", expand=True)
        self.pages["results"] = p

    # ─── Navigation ──────────────────────────────────────────────────────────

    def _show_page(self, key):
        for k, p in self.pages.items():
            p.pack_forget()
        self.pages[key].pack(fill="both", expand=True)
        # Update sidebar button colors
        for k, btn in self._nav_btns.items():
            btn.configure(
                fg_color=GOLD if k == key else "transparent",
                text_color=NAVY if k == key else TEXT_DIM,
                font=FU(13, "bold") if k == key else FU(13))
        self._current_page = key

    # ─── Run Logic ───────────────────────────────────────────────────────────

    def _browse_path(self):
        d = filedialog.askdirectory(title="Select breeze_data folder")
        if d:
            self.data_path_var.set(d)

    def _on_run(self):
        if self._running: return
        mode = getattr(self, "_current_page", "single")
        if mode == "results": mode = "single"

        from_d = self.from_date.get().strip()
        to_d   = self.to_date.get().strip()
        if not from_d or not to_d:
            messagebox.showwarning("Missing dates", "Please enter From and To dates.")
            return

        # Validate dates
        try:
            datetime.strptime(from_d, "%Y-%m-%d")
            datetime.strptime(to_d,   "%Y-%m-%d")
        except ValueError:
            messagebox.showerror("Invalid date", "Dates must be YYYY-MM-DD format.")
            return

        # Build command
        script = Path(__file__).parent / "main.py"
        data_path = self.data_path_var.get().strip()
        cmd = [sys.executable, str(script),
               "--data-path", data_path]

        if mode == "single":
            try:
                sp = self.single_panel.get_params()
            except ValueError as e:
                messagebox.showerror("Invalid parameters", str(e)); return

            cmd += ["single",
                    "--from", from_d, "--to", to_d,
                    "--atm-start", sp["atm_scan_start"],
                    "--atm-end",   sp["atm_scan_end"],
                    "--eod",       sp["eod_exit_time"],
                    "--atr-tf",    sp["atr_timeframe"],
                    "--atr-period", str(sp["atr_period"]),
                    "--atr-mult",  str(sp["atr_multiplier"]),
                    "--hedge-pct", str(sp["hedge_pct"]),
                    "--trail-step",str(sp["hedge_trail_step"]),
                    ]
        elif mode == "grid":
            try:
                gc = self.grid_panel.get_grid_config()
            except ValueError as e:
                messagebox.showerror("Invalid grid config", str(e)); return
            cmd += ["grid", "--from", from_d, "--to", to_d]
            if gc.get("fast_mode"):
                cmd.append("--fast")
        else:
            cmd += ["stats", "--from", from_d, "--to", to_d]

        self._start_process(cmd)

    def _on_stop(self):
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            self.log.append("\n⛔ Stopped by user.\n")
        self._set_idle()

    def _start_process(self, cmd):
        self._running = True
        self.run_btn.configure(state="disabled", fg_color=BORDER)
        self.stop_btn.configure(state="normal")
        self.status_lbl.configure(text="● Running…", text_color=GREEN)
        self.log.append(f"\n{'─'*60}\n▶  {' '.join(cmd)}\n{'─'*60}\n")

        def target():
            try:
                self._proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1,
                    cwd=str(Path(__file__).parent)
                )
                for line in self._proc.stdout:
                    self._q.put(("log", line))
                self._proc.wait()
                rc = self._proc.returncode
                self._q.put(("done", rc))
            except Exception as exc:
                self._q.put(("log", f"\nERROR: {exc}\n"))
                self._q.put(("done", -1))

        threading.Thread(target=target, daemon=True).start()

    def _poll_queue(self):
        try:
            while True:
                kind, data = self._q.get_nowait()
                if kind == "log":
                    self.log.append(data)
                    self._parse_metrics_from_log(data)
                elif kind == "done":
                    rc = data
                    self.log.append(f"\n{'─'*60}\n"
                                    f"{'✅ Completed' if rc == 0 else '❌ Exited (code '+str(rc)+')'}\n"
                                    f"{'─'*60}\n")
                    self._set_idle()
        except queue.Empty:
            pass
        self.after(80, self._poll_queue)

    def _set_idle(self):
        self._running = False
        self.run_btn.configure(state="normal", fg_color=GOLD)
        self.stop_btn.configure(state="disabled")
        self.status_lbl.configure(text="● Idle", text_color=TEXT_DIM)

    # ─── Metric scraping from console output ─────────────────────────────────

    _metrics_buf = {}

    def _parse_metrics_from_log(self, line: str):
        """Scrape key metrics from console output and update sidebar + results."""
        import re
        m = re.search(r"Total P&L:\s+₹([\d,\.\-]+)", line)
        if m:
            val = m.group(1).replace(",", "")
            try:
                pnl = float(val)
                color = GREEN if pnl >= 0 else RED
                self.sb_pnl.configure(text=f"₹{pnl:,.0f}", text_color=color)
                self._metrics_buf["total_pnl"] = pnl
            except: pass

        m = re.search(r"Win rate:\s+([\d\.]+)%", line)
        if m:
            wr = float(m.group(1))
            color = GREEN if wr >= 50 else ORANGE
            self.sb_wr.configure(text=f"{wr:.1f}%", text_color=color)
            self._metrics_buf["win_rate_pct"] = wr

        m = re.search(r"Sharpe.*?:\s+([\d\.\-]+)", line)
        if m:
            sh = float(m.group(1))
            self.sb_sh.configure(text=f"{sh:.3f}")
            self._metrics_buf["sharpe"] = sh

        m = re.search(r"Max drawdown:\s+₹([\d,\.\-]+)", line)
        if m:
            dd = float(m.group(1).replace(",", ""))
            self.sb_dd.configure(text=f"₹{dd:,.0f}", text_color=RED if dd < 0 else TEXT)
            self._metrics_buf["max_drawdown"] = dd

        # Also scrape other metrics for results table
        patterns = {
            "traded_days":      r"Traded days:\s+(\d+)",
            "skipped_days":     r"Skipped days:\s+(\d+)",
            "avg_daily_pnl":    r"Avg daily P&L:\s+₹([\d,\.\-]+)",
            "profit_factor":    r"Profit factor:\s+([\d\.]+)",
            "max_consec_losses":r"Max consec losses:\s+(\d+)",
        }
        for key, pat in patterns.items():
            m2 = re.search(pat, line)
            if m2:
                try:
                    v = m2.group(1).replace(",", "")
                    self._metrics_buf[key] = float(v)
                except: pass

        # When we get "Completed", populate results table
        if "Completed" in line or "✅" in line:
            if self._metrics_buf:
                self.results_table.populate(self._metrics_buf)


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Check dependencies
    try:
        import customtkinter
    except ImportError:
        print("Please install customtkinter:  pip install customtkinter")
        sys.exit(1)

    app = App()
    app.mainloop()
