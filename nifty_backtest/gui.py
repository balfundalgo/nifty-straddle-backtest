"""
gui.py — Balfund NIFTY Straddle Backtest GUI
Full-featured: live console, progress bar, detailed results, grid combination counter
Run: python gui.py
"""

import sys
import os
import threading
import queue
import json
from datetime import date
from pathlib import Path
import tkinter as tk
from tkinter import filedialog

import customtkinter as ctk

# ── Theme ─────────────────────────────────────────────────────────────────────
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

BG     = "#0d1117"
CARD   = "#161b22"
CARD2  = "#21262d"
BORDER = "#30363d"
GREEN  = "#238636"
GREEN2 = "#2ea043"
RED    = "#da3633"
BLUE   = "#58a6ff"
GOLD   = "#e3b341"
TEXT   = "#c9d1d9"
MUTED  = "#8b949e"
WHITE  = "#f0f6fc"

FH1  = ("Segoe UI", 15, "bold")
FH2  = ("Segoe UI", 12, "bold")
FB   = ("Segoe UI", 11)
FS   = ("Segoe UI", 10)
FM   = ("Consolas", 10)


class _QueueStream:
    def __init__(self, q): self._q = q
    def write(self, msg):
        if msg.strip(): self._q.put(("log", msg.rstrip()))
    def flush(self): pass


class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Balfund — NIFTY Straddle Backtest")
        self.geometry("1380x900")
        self.minsize(1200, 750)
        self.configure(fg_color=BG)

        self._q            = queue.Queue()
        self._running      = False
        self._thread       = None
        self._last_rpt     = None
        self._orig_stdout  = sys.stdout
        self._orig_stderr  = sys.stderr
        self._vars         = {}
        self._tab          = "single"

        self._build_ui()
        self._poll_queue()

    # ─── UI ──────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Header ────────────────────────────────────────────────────────
        hdr = ctk.CTkFrame(self, fg_color=CARD, corner_radius=0, height=54)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text="  📈  Balfund NIFTY Straddle Backtest",
                     font=FH1, text_color=BLUE).pack(side="left", padx=16)
        self._status_lbl = ctk.CTkLabel(hdr, text="● Idle",
                                         font=FB, text_color=MUTED)
        self._status_lbl.pack(side="right", padx=16)

        # ── Action bar ────────────────────────────────────────────────────
        act = ctk.CTkFrame(self, fg_color=CARD2, corner_radius=0, height=50)
        act.pack(fill="x")
        act.pack_propagate(False)

        self._run_btn = ctk.CTkButton(
            act, text="▶  RUN BACKTEST", font=("Segoe UI", 12, "bold"),
            width=190, height=36, corner_radius=6,
            fg_color=GREEN, hover_color=GREEN2,
            command=self._on_run
        )
        self._run_btn.pack(side="left", padx=(12, 6), pady=7)

        self._stop_btn = ctk.CTkButton(
            act, text="■  Stop", font=FB, width=80, height=36,
            corner_radius=6, fg_color=CARD, hover_color=RED,
            border_width=1, border_color=BORDER,
            command=self._on_stop
        )
        self._stop_btn.pack(side="left", padx=4, pady=7)

        self._open_btn = ctk.CTkButton(
            act, text="📂  Open Report", font=FB, width=140, height=36,
            corner_radius=6, fg_color=CARD, hover_color="#1f6feb",
            border_width=1, border_color=BORDER,
            command=self._open_report, state="disabled"
        )
        self._open_btn.pack(side="left", padx=4, pady=7)

        self._open_log_btn = ctk.CTkButton(
            act, text="📋  Open Trade Log", font=FB, width=150, height=36,
            corner_radius=6, fg_color=CARD, hover_color="#6e40c9",
            border_width=1, border_color=BORDER,
            command=self._open_trade_log, state="disabled"
        )
        self._open_log_btn.pack(side="left", padx=4, pady=7)

        # Progress bar + % label
        prog_frame = ctk.CTkFrame(act, fg_color="transparent")
        prog_frame.pack(side="right", padx=16, pady=7)
        self._pct_lbl = ctk.CTkLabel(prog_frame, text="0%", font=FS,
                                      text_color=MUTED, width=36)
        self._pct_lbl.pack(side="right", padx=(4, 0))
        self._prog = ctk.CTkProgressBar(prog_frame, width=280, height=8,
                                         progress_color=GREEN, fg_color=CARD)
        self._prog.pack(side="right")
        self._prog.set(0)

        # ── Body ──────────────────────────────────────────────────────────
        body = ctk.CTkFrame(self, fg_color=BG)
        body.pack(fill="both", expand=True, padx=10, pady=(8, 4))
        body.columnconfigure(0, weight=0)   # sidebar
        body.columnconfigure(1, weight=1)   # main panel
        body.rowconfigure(0, weight=1)

        # Sidebar tabs
        self._build_sidebar(body)
        # Main content (tab-switched)
        self._main = ctk.CTkFrame(body, fg_color=BG)
        self._main.grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        self._main.columnconfigure(0, weight=1)
        self._main.rowconfigure(0, weight=1)
        self._main.rowconfigure(1, weight=1)

        self._build_single_tab()
        self._build_grid_tab()
        self._build_results_tab()
        self._show_tab("single")

        # ── Console ───────────────────────────────────────────────────────
        con_frame = ctk.CTkFrame(self, fg_color=CARD, corner_radius=8)
        con_frame.pack(fill="x", padx=10, pady=(0, 6))

        con_hdr = ctk.CTkFrame(con_frame, fg_color="transparent")
        con_hdr.pack(fill="x", padx=10, pady=(6, 0))
        ctk.CTkLabel(con_hdr, text="Console Output",
                     font=("Segoe UI", 10, "bold"), text_color=GOLD).pack(side="left")
        ctk.CTkButton(con_hdr, text="Clear", font=FS, width=55, height=22,
                      fg_color=CARD2, hover_color=BORDER,
                      command=self._clear_console).pack(side="right")

        self._console = ctk.CTkTextbox(con_frame, height=170, font=FM,
                                        fg_color="#010409", text_color="#3fb950",
                                        corner_radius=4, border_width=1,
                                        border_color=BORDER)
        self._console.pack(fill="x", padx=8, pady=(2, 8))

    # ─── Sidebar ─────────────────────────────────────────────────────────────

    def _build_sidebar(self, parent):
        sb = ctk.CTkFrame(parent, fg_color=CARD, corner_radius=8, width=210)
        sb.grid(row=0, column=0, sticky="nsew")
        sb.pack_propagate(False)

        ctk.CTkLabel(sb, text="MODE", font=("Segoe UI", 9, "bold"),
                     text_color=MUTED).pack(pady=(14, 4), padx=12, anchor="w")

        for label, key in [("Single Run", "single"),
                            ("Grid Search", "grid"),
                            ("Results", "results")]:
            btn = ctk.CTkButton(
                sb, text=label, font=FB, height=36, corner_radius=6,
                fg_color="transparent", hover_color=CARD2, text_color=TEXT,
                anchor="w",
                command=lambda k=key: self._show_tab(k)
            )
            btn.pack(fill="x", padx=8, pady=2)
            setattr(self, f"_tab_btn_{key}", btn)

        # Mini stats
        ctk.CTkFrame(sb, fg_color=BORDER, height=1).pack(fill="x", padx=12, pady=12)
        ctk.CTkLabel(sb, text="LAST RUN", font=("Segoe UI", 9, "bold"),
                     text_color=MUTED).pack(padx=12, anchor="w")

        self._mini_stats = {}
        for label, key in [("Total P&L", "pnl"), ("Win Rate", "wr"),
                            ("Sharpe", "sharpe"), ("Max DD", "dd"),
                            ("Traded Days", "days")]:
            row = ctk.CTkFrame(sb, fg_color="transparent")
            row.pack(fill="x", padx=12, pady=1)
            ctk.CTkLabel(row, text=label, font=FS, text_color=MUTED,
                         width=90, anchor="w").pack(side="left")
            lbl = ctk.CTkLabel(row, text="—", font=("Segoe UI", 10, "bold"),
                               text_color=TEXT)
            lbl.pack(side="left")
            self._mini_stats[key] = lbl

    def _show_tab(self, name: str):
        self._tab = name
        for key in ["single", "grid", "results"]:
            btn = getattr(self, f"_tab_btn_{key}")
            btn.configure(fg_color=GREEN if key == name else "transparent")

        self._single_frame.grid_remove()
        self._grid_frame.grid_remove()
        self._results_frame.grid_remove()

        if name == "single":    self._single_frame.grid()
        elif name == "grid":    self._grid_frame.grid()
        elif name == "results": self._results_frame.grid()

    # ─── Single Run Tab ───────────────────────────────────────────────────────

    def _build_single_tab(self):
        self._single_frame = ctk.CTkScrollableFrame(
            self._main, fg_color=CARD, corner_radius=8,
            label_text="  Single Run — Strategy Parameters",
            label_font=FH2, label_text_color=BLUE
        )
        self._single_frame.grid(row=0, column=0, sticky="nsew", rowspan=2)
        self._single_frame.grid_remove()

        def section(text):
            ctk.CTkLabel(self._single_frame, text=text,
                         font=("Segoe UI", 11, "bold"),
                         text_color=GOLD).pack(anchor="w", padx=12, pady=(14, 2))
            ctk.CTkFrame(self._single_frame, fg_color=BORDER, height=1).pack(
                fill="x", padx=12, pady=(0, 4))

        def row(label, key, default, widget="entry", values=None, tooltip=None):
            r = ctk.CTkFrame(self._single_frame, fg_color="transparent")
            r.pack(fill="x", padx=12, pady=3)
            ctk.CTkLabel(r, text=label, font=FS, text_color=TEXT,
                         width=210, anchor="w").pack(side="left")
            if widget == "entry":
                e = ctk.CTkEntry(r, font=FS, height=30, width=150,
                                  fg_color=CARD2, border_color=BORDER, text_color=TEXT)
                e.insert(0, str(default))
                e.pack(side="left")
                self._vars[key] = e
            elif widget == "option":
                v = ctk.StringVar(value=default)
                o = ctk.CTkOptionMenu(r, values=values, variable=v,
                                       font=FS, height=30, width=150,
                                       fg_color=CARD2, button_color=CARD2,
                                       dropdown_fg_color=CARD,
                                       text_color=TEXT, dropdown_text_color=TEXT)
                o.pack(side="left")
                self._vars[key] = v

        # Data & Dates
        section("📁  Data & Date Range")
        path_row = ctk.CTkFrame(self._single_frame, fg_color="transparent")
        path_row.pack(fill="x", padx=12, pady=3)
        ctk.CTkLabel(path_row, text="Data Path", font=FS, text_color=TEXT,
                     width=210, anchor="w").pack(side="left")
        self._data_path = ctk.CTkEntry(path_row, font=FS, height=30,
                                        fg_color=CARD2, border_color=BORDER,
                                        text_color=TEXT)
        self._data_path.insert(0, r"C:\Users\Admin\Downloads\BreezeDownloader-v1.4.9\breeze_data")
        self._data_path.pack(side="left", fill="x", expand=True)
        ctk.CTkButton(path_row, text="…", width=34, height=30, font=FB,
                      fg_color=CARD2, command=self._browse).pack(side="left", padx=(4, 0))

        dr = ctk.CTkFrame(self._single_frame, fg_color="transparent")
        dr.pack(fill="x", padx=12, pady=3)
        ctk.CTkLabel(dr, text="From Date", font=FS, text_color=TEXT,
                     width=210, anchor="w").pack(side="left")
        self._from = ctk.CTkEntry(dr, font=FS, height=30, width=120,
                                   fg_color=CARD2, border_color=BORDER, text_color=TEXT)
        self._from.insert(0, "2026-01-02")
        self._from.pack(side="left")
        ctk.CTkLabel(dr, text="  To", font=FS, text_color=TEXT).pack(side="left")
        self._to = ctk.CTkEntry(dr, font=FS, height=30, width=120,
                                 fg_color=CARD2, border_color=BORDER, text_color=TEXT)
        self._to.insert(0, "2026-04-21")
        self._to.pack(side="left", padx=(4, 0))

        # ATM
        section("🎯  ATM Selection")
        row("Scan Start (HH:MM)",      "atm_scan_start",    "09:16")
        row("Scan End (HH:MM)",         "atm_scan_end",      "09:21")
        row("Max CE-PE Diff (₹)",       "max_premium_diff",  "20")

        # Hedge
        section("🛡️  Hedge")
        row("Hedge % of Premium",       "hedge_pct",         "0.05")
        row("Hedge Trail Step (₹)",     "hedge_trail_step",  "3.0")

        # VIX & SL
        section("📊  VIX & Stop Loss")
        row("VIX Intraday Threshold %", "vix_intraday_threshold", "3.0")
        row("SL Buffer (₹)",            "sl_buffer",         "5.0")

        # ATR
        section("📈  ATR Trailing (Surviving Leg)")
        row("ATR Timeframe",            "atr_timeframe",     "5min",
            widget="option", values=["1min", "5min", "15min"])
        row("ATR Period",               "atr_period",        "14")
        row("ATR Multiplier",           "atr_multiplier",    "1.5")

        # Exit & Position
        section("🚪  Exit & Position")
        row("EOD Exit Time (HH:MM)",    "eod_exit_time",     "15:20")
        row("Lot Size",                 "lot_size",          "75")

    # ─── Grid Search Tab ──────────────────────────────────────────────────────

    def _build_grid_tab(self):
        self._grid_frame = ctk.CTkFrame(self._main, fg_color=CARD, corner_radius=8)
        self._grid_frame.grid(row=0, column=0, sticky="nsew", rowspan=2)
        self._grid_frame.grid_remove()
        self._grid_frame.columnconfigure(0, weight=1)
        self._grid_frame.columnconfigure(1, weight=1)
        self._grid_frame.rowconfigure(1, weight=1)

        ctk.CTkLabel(self._grid_frame, text="  Grid Search — Parameter Ranges",
                     font=FH2, text_color=BLUE).grid(
            row=0, column=0, columnspan=2, sticky="w", padx=12, pady=10)

        # Combination counter
        ctr = ctk.CTkFrame(self._grid_frame, fg_color=CARD2, corner_radius=8)
        ctr.grid(row=0, column=1, sticky="e", padx=12, pady=8)
        ctk.CTkLabel(ctr, text="Total Combinations:", font=FS,
                     text_color=MUTED).pack(side="left", padx=8)
        self._combo_lbl = ctk.CTkLabel(ctr, text="—", font=("Segoe UI", 13, "bold"),
                                        text_color=GOLD)
        self._combo_lbl.pack(side="left", padx=(0, 8))

        # Left col: parameter ranges
        left = ctk.CTkScrollableFrame(self._grid_frame, fg_color="transparent",
                                       corner_radius=0)
        left.grid(row=1, column=0, sticky="nsew", padx=(8, 4), pady=(0, 8))

        def gsec(text):
            ctk.CTkLabel(left, text=text, font=("Segoe UI", 10, "bold"),
                         text_color=GOLD).pack(anchor="w", padx=8, pady=(10, 1))

        def grow(label, key, default, hint=""):
            r = ctk.CTkFrame(left, fg_color="transparent")
            r.pack(fill="x", padx=8, pady=2)
            ctk.CTkLabel(r, text=label, font=FS, text_color=TEXT,
                         width=170, anchor="w").pack(side="left")
            e = ctk.CTkEntry(r, font=FS, height=28, fg_color=CARD2,
                              border_color=BORDER, text_color=TEXT)
            e.insert(0, default)
            e.pack(side="left", fill="x", expand=True)
            if hint:
                ctk.CTkLabel(r, text=hint, font=("Segoe UI", 9),
                             text_color=MUTED).pack(side="left", padx=(4, 0))
            e.bind("<KeyRelease>", lambda _: self._update_combo_count())
            self._vars[key] = e

        gsec("ATM Selection")
        grow("Scan Starts",         "g_atm_start",   "09:16,09:17,09:18",   "HH:MM,...")
        grow("Scan Ends",           "g_atm_end",     "09:20,09:21",         "HH:MM,...")
        grow("Max Prem Diffs (₹)",  "g_prem_diff",   "10,20,30",            "₹,...")

        gsec("Hedge")
        grow("Hedge %s",            "g_hedge_pct",   "0.03,0.05,0.07",      "%,...")
        grow("Trail Steps (₹)",     "g_trail_step",  "2.0,3.0,4.0",         "₹,...")

        gsec("VIX")
        grow("VIX Thresholds %",    "g_vix_thr",     "2.0,3.0,4.0",         "%,...")

        gsec("ATR Trailing")
        grow("ATR Timeframes",      "g_atr_tf",      "1min,5min,15min",     "tf,...")
        grow("ATR Periods",         "g_atr_per",     "7,14,21",             "int,...")
        grow("ATR Multipliers",     "g_atr_mult",    "1.0,1.5,2.0",         "x,...")

        gsec("Exit")
        grow("EOD Times",           "g_eod",         "15:15,15:20,15:25",   "HH:MM,...")

        # Right col: fast mode + info
        right = ctk.CTkFrame(self._grid_frame, fg_color="transparent")
        right.grid(row=1, column=1, sticky="nsew", padx=(4, 8), pady=(0, 8))

        fast_card = ctk.CTkFrame(right, fg_color=CARD2, corner_radius=8)
        fast_card.pack(fill="x", pady=(0, 8))
        self._fast_var = ctk.BooleanVar(value=False)
        ctk.CTkSwitch(fast_card, text="  Fast Mode (reduced grid)",
                      variable=self._fast_var, font=FB, text_color=TEXT,
                      progress_color=GREEN,
                      command=self._update_combo_count).pack(padx=12, pady=10)
        ctk.CTkLabel(fast_card, text="Fast mode uses: ATR[5min], Period[14],\nMultiplier[1.0,1.5], EOD[15:20]",
                     font=FS, text_color=MUTED, justify="left").pack(padx=12, pady=(0, 10))

        info_card = ctk.CTkFrame(right, fg_color=CARD2, corner_radius=8)
        info_card.pack(fill="x")
        ctk.CTkLabel(info_card, text="ℹ️  Grid Search Info",
                     font=("Segoe UI", 11, "bold"), text_color=BLUE).pack(
            anchor="w", padx=12, pady=(10, 4))
        self._grid_info_lbl = ctk.CTkLabel(
            info_card,
            text="Enter comma-separated values for each\nparameter. All combinations will be tested.\n\nExample: '1min,5min,15min' tests all\nthree ATR timeframes.",
            font=FS, text_color=MUTED, justify="left"
        )
        self._grid_info_lbl.pack(anchor="w", padx=12, pady=(0, 10))

        self._update_combo_count()

    def _update_combo_count(self):
        try:
            if self._fast_var.get():
                total = 2 * 3 * 3 * 3 * 3 * 3  # fast mode fixed
                self._combo_lbl.configure(text=f"{total:,}", text_color=GOLD)
                return
            counts = []
            for key in ["g_atm_start", "g_atm_end", "g_prem_diff",
                        "g_hedge_pct", "g_trail_step", "g_vix_thr",
                        "g_atr_tf", "g_atr_per", "g_atr_mult", "g_eod"]:
                v = self._vars.get(key)
                if v:
                    parts = [x.strip() for x in v.get().split(",") if x.strip()]
                    counts.append(max(1, len(parts)))
            total = 1
            for c in counts: total *= c
            self._combo_lbl.configure(text=f"{total:,}",
                                       text_color=RED if total > 5000 else GOLD)
        except Exception:
            self._combo_lbl.configure(text="?", text_color=MUTED)

    # ─── Results Tab ─────────────────────────────────────────────────────────

    def _build_results_tab(self):
        self._results_frame = ctk.CTkFrame(self._main, fg_color=CARD, corner_radius=8)
        self._results_frame.grid(row=0, column=0, sticky="nsew", rowspan=2)
        self._results_frame.grid_remove()
        self._results_frame.columnconfigure(0, weight=1)
        self._results_frame.columnconfigure(1, weight=1)
        self._results_frame.rowconfigure(1, weight=1)

        ctk.CTkLabel(self._results_frame, text="  Results Summary",
                     font=FH2, text_color=BLUE).grid(
            row=0, column=0, columnspan=2, sticky="w", padx=12, pady=10)

        # Metrics cards (left)
        self._metrics_frame = ctk.CTkScrollableFrame(
            self._results_frame, fg_color="transparent", corner_radius=0)
        self._metrics_frame.grid(row=1, column=0, sticky="nsew", padx=(8, 4), pady=(0, 8))

        # Daily P&L table (right)
        right = ctk.CTkFrame(self._results_frame, fg_color="transparent")
        right.grid(row=1, column=1, sticky="nsew", padx=(4, 8), pady=(0, 8))
        right.rowconfigure(1, weight=1)

        ctk.CTkLabel(right, text="Daily P&L", font=("Segoe UI", 11, "bold"),
                     text_color=GOLD).grid(row=0, column=0, sticky="w", padx=4, pady=(0, 4))

        self._daily_box = ctk.CTkTextbox(right, font=("Consolas", 10),
                                          fg_color="#010409", text_color=TEXT,
                                          corner_radius=4, border_width=1,
                                          border_color=BORDER)
        self._daily_box.grid(row=1, column=0, sticky="nsew")
        right.columnconfigure(0, weight=1)

        self._show_placeholder_results()

    def _show_placeholder_results(self):
        self._daily_box.configure(state="normal")
        self._daily_box.delete("1.0", "end")
        self._daily_box.insert("end", "Run a backtest to see daily P&L here...\n")
        self._daily_box.configure(state="disabled")

    def _populate_results(self, metrics: dict, daily_rows: list):
        """Fill results tab with actual data after backtest."""
        # Clear old metric cards
        for w in self._metrics_frame.winfo_children():
            w.destroy()

        def mcard(label, value, color=TEXT):
            card = ctk.CTkFrame(self._metrics_frame, fg_color=CARD2, corner_radius=6)
            card.pack(fill="x", padx=4, pady=2)
            ctk.CTkLabel(card, text=label, font=FS, text_color=MUTED,
                         width=180, anchor="w").pack(side="left", padx=10, pady=6)
            ctk.CTkLabel(card, text=str(value), font=("Segoe UI", 11, "bold"),
                         text_color=color).pack(side="right", padx=10)

        pnl = metrics.get("total_pnl", 0)
        wr  = metrics.get("win_rate_pct", 0)

        ctk.CTkLabel(self._metrics_frame, text="PERFORMANCE",
                     font=("Segoe UI", 10, "bold"),
                     text_color=GOLD).pack(anchor="w", padx=4, pady=(8, 2))

        mcard("Total P&L",          f"₹{pnl:,.2f}",    GREEN if pnl >= 0 else RED)
        mcard("Traded Days",        metrics.get("traded_days", 0))
        mcard("Win Rate",           f"{wr:.1f}%",        GREEN if wr >= 50 else RED)
        mcard("Avg Daily P&L",      f"₹{metrics.get('avg_daily_pnl', 0):,.2f}")
        mcard("Sharpe (Annual)",    f"{metrics.get('sharpe', 0):.3f}",
              GREEN if metrics.get("sharpe", 0) > 1 else TEXT)

        ctk.CTkLabel(self._metrics_frame, text="RISK",
                     font=("Segoe UI", 10, "bold"),
                     text_color=GOLD).pack(anchor="w", padx=4, pady=(10, 2))

        mcard("Max Drawdown",       f"₹{metrics.get('max_drawdown', 0):,.2f}", RED)
        mcard("Max Drawdown %",     f"{metrics.get('max_drawdown_pct', 0):.1f}%", RED)
        mcard("Profit Factor",      f"{metrics.get('profit_factor', 0):.3f}")
        mcard("Risk:Reward Ratio",  f"1 : {metrics.get('recovery_ratio', 0):.2f}")

        ctk.CTkLabel(self._metrics_frame, text="STREAKS",
                     font=("Segoe UI", 10, "bold"),
                     text_color=GOLD).pack(anchor="w", padx=4, pady=(10, 2))

        mcard("Max Consec Profit Days", metrics.get("max_consec_wins", 0),   GREEN)
        mcard("Max Consec Loss Days",   metrics.get("max_consec_losses", 0), RED)
        mcard("Avg Win",            f"₹{metrics.get('avg_win', 0):,.2f}",   GREEN)
        mcard("Avg Loss",           f"₹{metrics.get('avg_loss', 0):,.2f}",  RED)
        mcard("Max Single Win",     f"₹{metrics.get('max_win', 0):,.2f}",   GREEN)
        mcard("Max Single Loss",    f"₹{metrics.get('max_loss', 0):,.2f}",  RED)

        ctk.CTkLabel(self._metrics_frame, text="EXIT ANALYSIS",
                     font=("Segoe UI", 10, "bold"),
                     text_color=GOLD).pack(anchor="w", padx=4, pady=(10, 2))

        mcard("Both Legs SL Hit",   metrics.get("both_legs_sl", 0), RED)
        mcard("One Leg SL Hit",     metrics.get("one_leg_sl", 0),   GOLD)
        mcard("EOD Exits",          metrics.get("eod_exits", 0),    MUTED)
        mcard("Skipped Days",       metrics.get("skipped_days", 0), MUTED)

        # Daily P&L table
        self._daily_box.configure(state="normal")
        self._daily_box.delete("1.0", "end")
        header = f"{'Date':<12} {'ATM':>6} {'CE SL':>8} {'PE SL':>8} {'P&L':>12} {'Cumul':>12}\n"
        self._daily_box.insert("end", header)
        self._daily_box.insert("end", "─" * 62 + "\n")

        cumul = 0
        for row in daily_rows:
            if row.get("status") != "ok":
                self._daily_box.insert(
                    "end",
                    f"{row.get('date',''):<12} {'SKIP':>6}  {row.get('notes','')[:30]}\n"
                )
                continue
            cumul += row.get("total_pnl", 0)
            pnl_val = row.get("total_pnl", 0)
            pnl_str = f"₹{pnl_val:>10,.0f}"
            cum_str = f"₹{cumul:>10,.0f}"
            line = (f"{row.get('date',''):<12} "
                    f"{str(row.get('atm_strike',''))!s:>6} "
                    f"{row.get('ce_exit_reason','')[:8]:>8} "
                    f"{row.get('pe_exit_reason','')[:8]:>8} "
                    f"{pnl_str:>12} "
                    f"{cum_str:>12}\n")
            self._daily_box.insert("end", line)

        self._daily_box.configure(state="disabled")

        # Update mini stats
        self._mini_stats["pnl"].configure(
            text=f"₹{pnl:,.0f}",
            text_color=GREEN if pnl >= 0 else RED)
        self._mini_stats["wr"].configure(text=f"{wr:.1f}%")
        self._mini_stats["sharpe"].configure(text=f"{metrics.get('sharpe',0):.3f}")
        self._mini_stats["dd"].configure(
            text=f"₹{metrics.get('max_drawdown',0):,.0f}", text_color=RED)
        self._mini_stats["days"].configure(text=str(metrics.get("traded_days", 0)))

    # ─── Events ──────────────────────────────────────────────────────────────

    def _browse(self):
        path = filedialog.askdirectory(title="Select breeze_data folder")
        if path:
            self._data_path.delete(0, "end")
            self._data_path.insert(0, path)

    def _on_run(self):
        if self._running:
            return
        self._running  = True
        self._last_rpt = None
        self._last_log = None
        self._open_btn.configure(state="disabled")
        self._open_log_btn.configure(state="disabled")
        self._run_btn.configure(state="disabled", text="⏳  Running...")
        self._status_lbl.configure(text="● Running", text_color=GOLD)
        self._prog.set(0)
        self._pct_lbl.configure(text="0%")
        self._clear_console()

        sys.stdout = _QueueStream(self._q)
        sys.stderr = _QueueStream(self._q)

        cfg = self._collect_cfg()
        self._thread = threading.Thread(
            target=self._worker, args=(cfg,), daemon=True)
        self._thread.start()

    def _on_stop(self):
        self._running = False
        self._q.put(("log", "⚠️  Stop requested — will halt after current day"))

    def _collect_cfg(self) -> dict:
        v = self._vars
        def g(k): return v[k].get()
        return {
            "mode":                   self._tab,
            "fast":                   self._fast_var.get(),
            "data_path":              self._data_path.get(),
            "from_date":              self._from.get(),
            "to_date":                self._to.get(),
            "atm_scan_start":         g("atm_scan_start"),
            "atm_scan_end":           g("atm_scan_end"),
            "max_premium_diff":       float(g("max_premium_diff")),
            "hedge_pct":              float(g("hedge_pct")),
            "vix_intraday_threshold": float(g("vix_intraday_threshold")),
            "sl_buffer":              float(g("sl_buffer")),
            "atr_timeframe":          g("atr_timeframe"),
            "atr_period":             int(g("atr_period")),
            "atr_multiplier":         float(g("atr_multiplier")),
            "hedge_trail_step":       float(g("hedge_trail_step")),
            "eod_exit_time":          g("eod_exit_time"),
            "lot_size":               int(g("lot_size")),
            "g_atm_start":   g("g_atm_start"),   "g_atm_end":  g("g_atm_end"),
            "g_prem_diff":   g("g_prem_diff"),   "g_hedge_pct":g("g_hedge_pct"),
            "g_trail_step":  g("g_trail_step"),  "g_vix_thr":  g("g_vix_thr"),
            "g_atr_tf":      g("g_atr_tf"),      "g_atr_per":  g("g_atr_per"),
            "g_atr_mult":    g("g_atr_mult"),    "g_eod":      g("g_eod"),
        }

    # ─── Worker ──────────────────────────────────────────────────────────────

    def _worker(self, cfg: dict):
        try:
            from gui_runner import run_backtest
            result = run_backtest(
                cfg,
                progress_fn=self._set_progress,
                results_fn=lambda m, d: self.after(0, lambda: self._populate_results(m, d))
            )
            if result:
                self._last_rpt = result.get("report")
                self._last_log = result.get("trade_log")
                if self._last_rpt:
                    self.after(0, lambda: self._open_btn.configure(state="normal"))
                if self._last_log:
                    self.after(0, lambda: self._open_log_btn.configure(state="normal"))
                self.after(0, lambda: self._show_tab("results"))
        except Exception as e:
            import traceback
            print(f"❌ Error: {e}")
            print(traceback.format_exc())
        finally:
            sys.stdout = self._orig_stdout
            sys.stderr = self._orig_stderr
            self._running = False
            self.after(0, self._run_done)

    def _set_progress(self, val: float):
        self.after(0, lambda: self._prog.set(val))
        self.after(0, lambda: self._pct_lbl.configure(text=f"{int(val*100)}%"))

    def _run_done(self):
        self._run_btn.configure(state="normal", text="▶  RUN BACKTEST")
        self._status_lbl.configure(text="● Done", text_color=GREEN)
        self._prog.set(1)
        self._pct_lbl.configure(text="100%")

    # ─── Queue poll ──────────────────────────────────────────────────────────

    def _poll_queue(self):
        try:
            while True:
                item = self._q.get_nowait()
                kind, msg = item
                if kind == "log":
                    self._console.configure(state="normal")
                    self._console.insert("end", msg + "\n")
                    self._console.see("end")
                    self._console.configure(state="disabled")
        except queue.Empty:
            pass
        self.after(80, self._poll_queue)

    def _clear_console(self):
        self._console.configure(state="normal")
        self._console.delete("1.0", "end")
        self._console.configure(state="disabled")

    def _open_report(self):
        if self._last_rpt and Path(self._last_rpt).exists():
            os.startfile(self._last_rpt)

    def _open_trade_log(self):
        if self._last_log and Path(self._last_log).exists():
            os.startfile(self._last_log)


if __name__ == "__main__":
    app = App()
    app.mainloop()
