"""
gui.py — Balfund NIFTY Straddle Backtest GUI
Drop into nifty_backtest/ folder alongside strategy.py, config.py etc.
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

# ── Appearance ────────────────────────────────────────────────────────────────
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

BG          = "#0d1117"
CARD        = "#161b22"
BORDER      = "#30363d"
ACCENT      = "#238636"
ACCENT_HOVER= "#2ea043"
RED         = "#da3633"
BLUE        = "#58a6ff"
GOLD        = "#e3b341"
TEXT        = "#c9d1d9"
MUTED       = "#8b949e"

FONT_H1     = ("Segoe UI", 15, "bold")
FONT_H2     = ("Segoe UI", 12, "bold")
FONT_BODY   = ("Segoe UI", 11)
FONT_SMALL  = ("Segoe UI", 10)
FONT_MONO   = ("Consolas", 10)


# ── Stdout redirect ───────────────────────────────────────────────────────────
class _QueueStream:
    """Redirect print() output to a queue so GUI can display it."""
    def __init__(self, q: queue.Queue):
        self._q = q
    def write(self, msg: str):
        if msg.strip():
            self._q.put(msg.rstrip())
    def flush(self):
        pass


# ── Main App ──────────────────────────────────────────────────────────────────
class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Balfund — NIFTY Straddle Backtest")
        self.geometry("1280x820")
        self.minsize(1100, 720)
        self.configure(fg_color=BG)

        self._q         = queue.Queue()
        self._running   = False
        self._thread    = None
        self._last_rpt  = None
        self._orig_stdout = sys.stdout
        self._orig_stderr = sys.stderr

        self._build_ui()
        self._poll_queue()

    # ─── UI ──────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # Header
        hdr = ctk.CTkFrame(self, fg_color=CARD, corner_radius=0, height=52)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text="  📈  Balfund NIFTY Straddle Backtest",
                     font=FONT_H1, text_color=BLUE).pack(side="left", padx=16)
        self._status = ctk.CTkLabel(hdr, text="● Idle",
                                     font=FONT_BODY, text_color=MUTED)
        self._status.pack(side="right", padx=16)

        # Body
        body = ctk.CTkFrame(self, fg_color=BG)
        body.pack(fill="both", expand=True, padx=10, pady=8)
        body.columnconfigure(0, weight=3)
        body.columnconfigure(1, weight=2)
        body.rowconfigure(0, weight=1)

        self._build_left(body)
        self._build_right(body)

        # Log
        log_card = ctk.CTkFrame(self, fg_color=CARD, corner_radius=8)
        log_card.pack(fill="x", padx=10, pady=(0, 6))
        ctk.CTkLabel(log_card, text=" Console Output",
                     font=FONT_SMALL, text_color=MUTED).pack(anchor="w", padx=10, pady=(6, 0))
        self._log = ctk.CTkTextbox(log_card, height=150, font=FONT_MONO,
                                    fg_color="#010409", text_color="#3fb950",
                                    corner_radius=4, border_width=1,
                                    border_color=BORDER)
        self._log.pack(fill="x", padx=8, pady=(2, 8))

        # Bottom bar
        bar = ctk.CTkFrame(self, fg_color=CARD, corner_radius=0, height=52)
        bar.pack(fill="x", side="bottom")
        bar.pack_propagate(False)

        self._run_btn = ctk.CTkButton(
            bar, text="▶  RUN BACKTEST", font=("Segoe UI", 12, "bold"),
            width=180, height=36, corner_radius=6,
            fg_color=ACCENT, hover_color=ACCENT_HOVER,
            command=self._on_run
        )
        self._run_btn.pack(side="left", padx=(12, 6), pady=8)

        self._stop_btn = ctk.CTkButton(
            bar, text="■  Stop", font=FONT_BODY, width=80, height=36,
            corner_radius=6, fg_color="#21262d", hover_color=RED,
            command=self._on_stop
        )
        self._stop_btn.pack(side="left", padx=4, pady=8)

        self._open_btn = ctk.CTkButton(
            bar, text="📂  Open Report", font=FONT_BODY, width=130, height=36,
            corner_radius=6, fg_color="#21262d", hover_color="#1f6feb",
            command=self._open_report, state="disabled"
        )
        self._open_btn.pack(side="left", padx=4, pady=8)

        self._prog = ctk.CTkProgressBar(bar, width=260, height=6,
                                         progress_color=ACCENT,
                                         fg_color="#21262d")
        self._prog.pack(side="right", padx=16, pady=8)
        self._prog.set(0)

    def _build_left(self, parent):
        frame = ctk.CTkScrollableFrame(parent, fg_color=CARD, corner_radius=8,
                                        label_text="  Strategy Parameters",
                                        label_font=FONT_H2,
                                        label_text_color=BLUE)
        frame.grid(row=0, column=0, sticky="nsew", padx=(0, 5))
        self._vars = {}

        def section(title):
            ctk.CTkLabel(frame, text=title, font=("Segoe UI", 11, "bold"),
                         text_color=GOLD).pack(anchor="w", padx=10, pady=(12, 2))

        def row(label, key, default, widget="entry", values=None):
            r = ctk.CTkFrame(frame, fg_color="transparent")
            r.pack(fill="x", padx=10, pady=2)
            ctk.CTkLabel(r, text=label, font=FONT_SMALL, text_color=TEXT,
                         width=200, anchor="w").pack(side="left")
            if widget == "entry":
                e = ctk.CTkEntry(r, font=FONT_SMALL, height=28, width=160,
                                  fg_color="#0d1117", border_color=BORDER,
                                  text_color=TEXT)
                e.insert(0, str(default))
                e.pack(side="left")
                self._vars[key] = e
            elif widget == "option":
                v = ctk.StringVar(value=default)
                o = ctk.CTkOptionMenu(r, values=values, variable=v,
                                       font=FONT_SMALL, height=28, width=160,
                                       fg_color="#0d1117", button_color="#21262d",
                                       dropdown_fg_color="#161b22",
                                       text_color=TEXT)
                o.pack(side="left")
                self._vars[key] = v
            elif widget == "switch":
                v = ctk.BooleanVar(value=default)
                s = ctk.CTkSwitch(r, variable=v, text="", onvalue=True, offvalue=False,
                                   progress_color=ACCENT)
                s.pack(side="left")
                self._vars[key] = v

        # ATM Selection
        section("🎯 ATM Selection")
        row("Scan Start (HH:MM)",     "atm_scan_start",    "09:16")
        row("Scan End (HH:MM)",        "atm_scan_end",      "09:21")
        row("Max CE-PE Diff (₹)",      "max_premium_diff",  "20")

        # Hedge
        section("🛡️ Hedge")
        row("Hedge % of Premium",      "hedge_pct",         "0.05")
        row("Step Trail Size (₹)",     "hedge_trail_step",  "3.0")

        # VIX / SL
        section("📊 VIX & Stop Loss")
        row("VIX Intraday Threshold %","vix_intraday_threshold", "3.0")
        row("SL Buffer (₹)",           "sl_buffer",         "5.0")

        # ATR Trailing
        section("📈 ATR Trailing")
        row("ATR Timeframe",           "atr_timeframe",     "5min",
            widget="option", values=["1min", "5min", "15min"])
        row("ATR Period",              "atr_period",        "14")
        row("ATR Multiplier",          "atr_multiplier",    "1.5")

        # Exit
        section("🚪 Exit")
        row("EOD Exit Time (HH:MM)",   "eod_exit_time",     "15:20")
        row("Lot Size",                "lot_size",          "75")

        # Grid Search
        section("🔢 Grid Search Ranges (comma-separated)")
        row("ATR Timeframes",          "grid_atr_tf",       "1min,5min,15min")
        row("ATR Periods",             "grid_atr_per",      "7,14,21")
        row("ATR Multipliers",         "grid_atr_mult",     "1.0,1.5,2.0")
        row("Hedge %s",                "grid_hedge_pct",    "0.03,0.05,0.07")
        row("EOD Times",               "grid_eod",          "15:15,15:20,15:25")
        row("Max Prem Diffs",          "grid_prem_diff",    "10,20,30")

    def _build_right(self, parent):
        frame = ctk.CTkFrame(parent, fg_color=CARD, corner_radius=8)
        frame.grid(row=0, column=1, sticky="nsew", padx=(5, 0))

        def label(text, color=MUTED):
            ctk.CTkLabel(frame, text=text, font=FONT_SMALL,
                         text_color=color).pack(anchor="w", padx=12, pady=(10, 1))

        # Data path
        label("DATA PATH", GOLD)
        path_row = ctk.CTkFrame(frame, fg_color="transparent")
        path_row.pack(fill="x", padx=10, pady=(0, 4))
        self._data_path = ctk.CTkEntry(path_row, font=FONT_SMALL, height=30,
                                        fg_color="#0d1117", border_color=BORDER,
                                        text_color=TEXT)
        self._data_path.insert(0, r"C:\Users\Admin\Downloads\BreezeDownloader-v1.4.9\breeze_data")
        self._data_path.pack(side="left", fill="x", expand=True)
        ctk.CTkButton(path_row, text="…", width=32, height=30, font=FONT_BODY,
                      fg_color="#21262d", command=self._browse).pack(side="left", padx=(4, 0))

        # Dates
        label("FROM DATE")
        self._from = ctk.CTkEntry(frame, font=FONT_SMALL, height=30, width=140,
                                   fg_color="#0d1117", border_color=BORDER, text_color=TEXT)
        self._from.insert(0, "2026-01-02")
        self._from.pack(anchor="w", padx=12)

        label("TO DATE")
        self._to = ctk.CTkEntry(frame, font=FONT_SMALL, height=30, width=140,
                                 fg_color="#0d1117", border_color=BORDER, text_color=TEXT)
        self._to.insert(0, "2026-04-21")
        self._to.pack(anchor="w", padx=12)

        # Mode
        label("BACKTEST MODE", GOLD)
        self._mode = ctk.CTkSegmentedButton(
            frame, values=["Single Run", "Grid Search", "Fast Grid"],
            font=FONT_SMALL, height=32,
            selected_color=ACCENT, selected_hover_color=ACCENT_HOVER,
            unselected_color="#21262d", unselected_hover_color="#30363d",
            text_color=TEXT
        )
        self._mode.set("Single Run")
        self._mode.pack(fill="x", padx=12, pady=(2, 8))

        # Results summary
        label("RESULTS SUMMARY", GOLD)
        self._summary = ctk.CTkTextbox(frame, font=FONT_MONO,
                                        fg_color="#010409", text_color=TEXT,
                                        corner_radius=4, border_width=1,
                                        border_color=BORDER)
        self._summary.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self._summary.insert("end", "Results will appear here after run...\n")
        self._summary.configure(state="disabled")

    # ─── Events ──────────────────────────────────────────────────────────────

    def _browse(self):
        path = filedialog.askdirectory(title="Select breeze_data folder")
        if path:
            self._data_path.delete(0, "end")
            self._data_path.insert(0, path)

    def _on_run(self):
        if self._running:
            return
        self._running = True
        self._last_rpt = None
        self._open_btn.configure(state="disabled")
        self._run_btn.configure(state="disabled", text="⏳  Running...")
        self._status.configure(text="● Running", text_color=GOLD)
        self._prog.set(0)
        self._clear_log()

        # Redirect stdout → queue
        sys.stdout = _QueueStream(self._q)
        sys.stderr = _QueueStream(self._q)

        cfg = self._collect()
        self._thread = threading.Thread(target=self._worker, args=(cfg,), daemon=True)
        self._thread.start()

    def _on_stop(self):
        self._running = False
        self._q.put("⚠️  Stop requested")

    def _collect(self) -> dict:
        v = self._vars
        def get(k): return v[k].get() if isinstance(v[k], (ctk.CTkEntry,)) else v[k].get()
        return {
            "data_path":              self._data_path.get(),
            "from_date":              self._from.get(),
            "to_date":                self._to.get(),
            "mode":                   self._mode.get(),
            "atm_scan_start":         get("atm_scan_start"),
            "atm_scan_end":           get("atm_scan_end"),
            "max_premium_diff":       float(get("max_premium_diff")),
            "hedge_pct":              float(get("hedge_pct")),
            "vix_intraday_threshold": float(get("vix_intraday_threshold")),
            "sl_buffer":              float(get("sl_buffer")),
            "atr_timeframe":          get("atr_timeframe"),
            "atr_period":             int(get("atr_period")),
            "atr_multiplier":         float(get("atr_multiplier")),
            "hedge_trail_step":       float(get("hedge_trail_step")),
            "eod_exit_time":          get("eod_exit_time"),
            "lot_size":               int(get("lot_size")),
            "grid_atr_tf":            get("grid_atr_tf"),
            "grid_atr_per":           get("grid_atr_per"),
            "grid_atr_mult":          get("grid_atr_mult"),
            "grid_hedge_pct":         get("grid_hedge_pct"),
            "grid_eod":               get("grid_eod"),
            "grid_prem_diff":         get("grid_prem_diff"),
        }

    # ─── Worker (background thread) ──────────────────────────────────────────

    def _worker(self, cfg: dict):
        try:
            from gui_runner import run_backtest
            report = run_backtest(cfg, lambda v: self.after(0, lambda: self._prog.set(v)))
            if report:
                self._last_rpt = report
                self.after(0, lambda: self._open_btn.configure(state="normal"))
        except Exception as e:
            import traceback
            print(f"❌ Error: {e}")
            print(traceback.format_exc())
        finally:
            sys.stdout = self._orig_stdout
            sys.stderr = self._orig_stderr
            self._running = False
            self.after(0, self._done)

    def _done(self):
        self._run_btn.configure(state="normal", text="▶  RUN BACKTEST")
        self._status.configure(text="● Done", text_color=ACCENT)
        self._prog.set(1)

    # ─── Log ─────────────────────────────────────────────────────────────────

    def _poll_queue(self):
        try:
            while True:
                msg = self._q.get_nowait()
                self._log.configure(state="normal")
                self._log.insert("end", msg + "\n")
                self._log.see("end")
                self._log.configure(state="disabled")

                # Mirror metrics to summary panel
                if any(k in msg for k in ["Total P&L", "Win rate", "Sharpe",
                                            "Profit factor", "Traded days",
                                            "Max drawdown", "═", "─"]):
                    self._summary.configure(state="normal")
                    self._summary.insert("end", msg + "\n")
                    self._summary.see("end")
                    self._summary.configure(state="disabled")
        except queue.Empty:
            pass
        self.after(100, self._poll_queue)

    def _clear_log(self):
        self._log.configure(state="normal")
        self._log.delete("1.0", "end")
        self._log.configure(state="disabled")
        self._summary.configure(state="normal")
        self._summary.delete("1.0", "end")
        self._summary.configure(state="disabled")

    def _open_report(self):
        if self._last_rpt and Path(self._last_rpt).exists():
            os.startfile(self._last_rpt)


if __name__ == "__main__":
    app = App()
    app.mainloop()
