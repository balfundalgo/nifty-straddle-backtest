"""
gui.py - Balfund NIFTY Straddle Backtest GUI
ThreadPoolExecutor for parallelism (safe with Tkinter on Windows).
Build EXE: push to GitHub, Actions builds BalfundBacktest.exe
"""
import sys, os, threading, queue, multiprocessing
from pathlib import Path
from tkinter import filedialog
import customtkinter as ctk
from trial_lock import check_trial
check_trial()

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

BG    = "#0d1117"; CARD  = "#161b22"; CARD2 = "#21262d"; BORDER= "#30363d"
GREEN = "#238636"; GREEN2= "#2ea043"; RED   = "#da3633"; BLUE  = "#58a6ff"
GOLD  = "#e3b341"; TEXT  = "#c9d1d9"; MUTED = "#8b949e"
FH1=("Segoe UI",15,"bold"); FH2=("Segoe UI",12,"bold"); FB=("Segoe UI",11)
FS=("Segoe UI",10); FM=("Consolas",10)
MAX_CORES = multiprocessing.cpu_count()

class QueueStream:
    def __init__(self, q): self._q = q
    def write(self, m):
        if m.strip(): self._q.put(m.rstrip())
    def flush(self): pass

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Balfund \u2014 NIFTY Straddle Backtest")
        self.geometry("1350x880"); self.minsize(1100,750)
        self.configure(fg_color=BG)
        self._q=queue.Queue(); self._running=False
        self._last_report=None; self._last_log=None
        self._orig_out=sys.stdout; self._orig_err=sys.stderr
        self._vars={}; self._active_tab="single"
        self._build(); self._poll()

    def _build(self):
        # Header
        hdr=ctk.CTkFrame(self,fg_color=CARD,corner_radius=0,height=52)
        hdr.pack(fill="x"); hdr.pack_propagate(False)
        ctk.CTkLabel(hdr,text="  \U0001f4c8  Balfund NIFTY Straddle Backtest",font=FH1,text_color=BLUE).pack(side="left",padx=16)
        self._status=ctk.CTkLabel(hdr,text="\u25cf Idle",font=FB,text_color=MUTED)
        self._status.pack(side="right",padx=16)
        # Action bar
        act=ctk.CTkFrame(self,fg_color=CARD2,corner_radius=0,height=50)
        act.pack(fill="x"); act.pack_propagate(False)
        self._run_btn=ctk.CTkButton(act,text="\u25b6  RUN BACKTEST",font=("Segoe UI",12,"bold"),
            width=190,height=36,corner_radius=6,fg_color=GREEN,hover_color=GREEN2,command=self._on_run)
        self._run_btn.pack(side="left",padx=(12,6),pady=7)
        ctk.CTkButton(act,text="\u25a0  Stop",font=FB,width=80,height=36,corner_radius=6,
            fg_color=CARD,hover_color=RED,border_width=1,border_color=BORDER,command=self._on_stop).pack(side="left",padx=4,pady=7)
        self._open_btn=ctk.CTkButton(act,text="\U0001f4ca  Open Report",font=FB,width=130,height=36,
            corner_radius=6,fg_color=CARD,hover_color="#1f6feb",border_width=1,border_color=BORDER,
            command=self._open_report,state="disabled")
        self._open_btn.pack(side="left",padx=4,pady=7)
        self._log_btn=ctk.CTkButton(act,text="\U0001f4cb  Trade Log",font=FB,width=110,height=36,
            corner_radius=6,fg_color=CARD,hover_color="#6e40c9",border_width=1,border_color=BORDER,
            command=self._open_log,state="disabled")
        self._log_btn.pack(side="left",padx=4,pady=7)
        pf=ctk.CTkFrame(act,fg_color="transparent"); pf.pack(side="right",padx=12,pady=7)
        self._pct=ctk.CTkLabel(pf,text="0%",font=FS,text_color=MUTED,width=38); self._pct.pack(side="right",padx=(4,0))
        self._prog=ctk.CTkProgressBar(pf,width=260,height=8,progress_color=GREEN,fg_color=CARD)
        self._prog.pack(side="right"); self._prog.set(0)
        # Body
        body=ctk.CTkFrame(self,fg_color=BG); body.pack(fill="both",expand=True,padx=10,pady=(8,4))
        body.columnconfigure(0,weight=0); body.columnconfigure(1,weight=1); body.rowconfigure(0,weight=1)
        self._build_sidebar(body)
        self._main=ctk.CTkFrame(body,fg_color=BG)
        self._main.grid(row=0,column=1,sticky="nsew",padx=(6,0))
        self._main.columnconfigure(0,weight=1); self._main.rowconfigure(0,weight=1)
        self._build_single(); self._build_grid(); self._build_results()
        self._show("single")
        # Console
        con=ctk.CTkFrame(self,fg_color=CARD,corner_radius=8); con.pack(fill="x",padx=10,pady=(0,6))
        ch=ctk.CTkFrame(con,fg_color="transparent"); ch.pack(fill="x",padx=10,pady=(6,0))
        ctk.CTkLabel(ch,text="Console Output",font=("Segoe UI",10,"bold"),text_color=GOLD).pack(side="left")
        ctk.CTkButton(ch,text="Clear",font=FS,width=55,height=22,fg_color=CARD2,hover_color=BORDER,
            command=self._clear_con).pack(side="right")
        self._con=ctk.CTkTextbox(con,height=170,font=FM,fg_color="#010409",text_color="#3fb950",
            corner_radius=4,border_width=1,border_color=BORDER)
        self._con.pack(fill="x",padx=8,pady=(2,8))

    def _build_sidebar(self,parent):
        sb=ctk.CTkFrame(parent,fg_color=CARD,corner_radius=8,width=200)
        sb.grid(row=0,column=0,sticky="nsew"); sb.pack_propagate(False)
        ctk.CTkLabel(sb,text="MODE",font=("Segoe UI",9,"bold"),text_color=MUTED).pack(pady=(14,4),padx=12,anchor="w")
        for lbl,key in [("Single Run","single"),("Grid Search","grid"),("Results","results")]:
            btn=ctk.CTkButton(sb,text=lbl,font=FB,height=36,corner_radius=6,fg_color="transparent",
                hover_color=CARD2,text_color=TEXT,anchor="w",command=lambda k=key: self._show(k))
            btn.pack(fill="x",padx=8,pady=2); setattr(self,f"_tab_{key}",btn)
        ctk.CTkFrame(sb,fg_color=BORDER,height=1).pack(fill="x",padx=12,pady=10)
        ctk.CTkLabel(sb,text="LAST RUN",font=("Segoe UI",9,"bold"),text_color=MUTED).pack(padx=12,anchor="w")
        self._mini={}
        for lbl,key in [("Total P&L","pnl"),("Win Rate","wr"),("Sharpe","sharpe"),("Max DD","dd"),("Days","days")]:
            r=ctk.CTkFrame(sb,fg_color="transparent"); r.pack(fill="x",padx=12,pady=1)
            ctk.CTkLabel(r,text=lbl,font=FS,text_color=MUTED,width=70,anchor="w").pack(side="left")
            lv=ctk.CTkLabel(r,text="\u2014",font=("Segoe UI",10,"bold"),text_color=TEXT); lv.pack(side="left")
            self._mini[key]=lv

    def _show(self,name):
        self._active_tab=name
        for k in ["single","grid","results"]:
            getattr(self,f"_tab_{k}").configure(fg_color=GREEN if k==name else "transparent")
        self._sf.grid_remove(); self._gf.grid_remove(); self._rf.grid_remove()
        {"single":self._sf,"grid":self._gf,"results":self._rf}[name].grid()

    def _e(self,parent,key,default,width=150):
        e=ctk.CTkEntry(parent,font=FS,height=30,width=width,fg_color=CARD2,border_color=BORDER,text_color=TEXT)
        e.insert(0,str(default)); self._vars[key]=e; return e

    def _build_single(self):
        self._sf=ctk.CTkScrollableFrame(self._main,fg_color=CARD,corner_radius=8,
            label_text="  Single Run \u2014 Strategy Parameters",label_font=FH2,label_text_color=BLUE)
        self._sf.grid(row=0,column=0,sticky="nsew"); self._sf.grid_remove()
        def sec(t):
            ctk.CTkLabel(self._sf,text=t,font=("Segoe UI",11,"bold"),text_color=GOLD).pack(anchor="w",padx=12,pady=(12,2))
            ctk.CTkFrame(self._sf,fg_color=BORDER,height=1).pack(fill="x",padx=12,pady=(0,4))
        def row(lbl,key,default,widget="entry",values=None):
            r=ctk.CTkFrame(self._sf,fg_color="transparent"); r.pack(fill="x",padx=12,pady=3)
            ctk.CTkLabel(r,text=lbl,font=FS,text_color=TEXT,width=225,anchor="w").pack(side="left")
            if widget=="entry": self._e(r,key,default).pack(side="left")
            else:
                v=ctk.StringVar(value=default)
                ctk.CTkOptionMenu(r,values=values,variable=v,font=FS,height=30,width=150,
                    fg_color=CARD2,button_color=CARD2,text_color=TEXT,
                    dropdown_fg_color=CARD,dropdown_text_color=TEXT).pack(side="left")
                self._vars[key]=v
        sec("\U0001f4c1  Data & Date Range")
        pr=ctk.CTkFrame(self._sf,fg_color="transparent"); pr.pack(fill="x",padx=12,pady=3)
        ctk.CTkLabel(pr,text="Data Path",font=FS,text_color=TEXT,width=225,anchor="w").pack(side="left")
        self._dp=ctk.CTkEntry(pr,font=FS,height=30,fg_color=CARD2,border_color=BORDER,text_color=TEXT)
        self._dp.insert(0,r"C:\Users\Admin\Downloads\BreezeDownloader-v1.5.0\breeze_data")
        self._dp.pack(side="left",fill="x",expand=True)
        ctk.CTkButton(pr,text="\u2026",width=34,height=30,font=FB,fg_color=CARD2,command=self._browse).pack(side="left",padx=(4,0))
        dr=ctk.CTkFrame(self._sf,fg_color="transparent"); dr.pack(fill="x",padx=12,pady=3)
        ctk.CTkLabel(dr,text="Date Range",font=FS,text_color=TEXT,width=225,anchor="w").pack(side="left")
        self._frm=ctk.CTkEntry(dr,font=FS,height=30,width=115,fg_color=CARD2,border_color=BORDER,text_color=TEXT)
        self._frm.insert(0,"2026-01-02"); self._frm.pack(side="left")
        ctk.CTkLabel(dr,text="  to  ",font=FS,text_color=MUTED).pack(side="left")
        self._to=ctk.CTkEntry(dr,font=FS,height=30,width=115,fg_color=CARD2,border_color=BORDER,text_color=TEXT)
        self._to.insert(0,"2026-04-21"); self._to.pack(side="left")
        sec("\U0001f3af  ATM Selection")
        row("Scan Start (HH:MM)","atm_scan_start","09:16"); row("Scan End (HH:MM)","atm_scan_end","09:21")
        row("Max CE-PE Diff (\u20b9)","max_premium_diff","20.0")
        sec("\U0001f6e1\ufe0f  Hedge")
        row("Hedge % of Premium","hedge_pct","0.05"); row("Hedge Trail Step (\u20b9)","hedge_trail_step","3.0")
        sec("\U0001f4ca  VIX & Stop Loss")
        row("VIX Intraday Threshold %","vix_intraday_threshold","3.0")
        row("SL % VIX < 12","sl_pct_vix_lt12","0.40"); row("SL % VIX 12-16 Calm","sl_pct_vix_12_16_calm","0.40")
        row("SL % VIX 12-16 Volatile","sl_pct_vix_12_16_volatile","0.25")
        row("SL % VIX 16-20","sl_pct_vix_16_20","0.25"); row("SL % VIX > 20","sl_pct_vix_gt20","0.15")
        row("SL Buffer (\u20b9)","sl_buffer","5.0")
        sec("\U0001f4c8  ATR Trailing")
        row("ATR Timeframe","atr_timeframe","5min",widget="option",values=["1min","3min","5min","15min","30min"])
        row("ATR Period","atr_period","14"); row("ATR Multiplier","atr_multiplier","1.5")
        sec("\U0001f6aa  Exit & Slippage")
        row("EOD Exit Time (HH:MM)","eod_exit_time","15:20"); row("Lot Size","lot_size","75")
        row("Slippage %","slippage_pct","0.001")

    def _build_grid(self):
        self._gf=ctk.CTkFrame(self._main,fg_color=CARD,corner_radius=8)
        self._gf.grid(row=0,column=0,sticky="nsew"); self._gf.grid_remove()
        self._gf.columnconfigure(0,weight=1); self._gf.columnconfigure(1,weight=1); self._gf.rowconfigure(1,weight=1)
        ctk.CTkLabel(self._gf,text="  Grid Search \u2014 Parameter Ranges",font=FH2,text_color=BLUE).grid(row=0,column=0,sticky="w",padx=12,pady=10)
        ctr=ctk.CTkFrame(self._gf,fg_color=CARD2,corner_radius=8); ctr.grid(row=0,column=1,sticky="e",padx=12,pady=8)
        ctk.CTkLabel(ctr,text="Total Combos:",font=FS,text_color=MUTED).pack(side="left",padx=8)
        self._clbl=ctk.CTkLabel(ctr,text="\u2014",font=("Segoe UI",13,"bold"),text_color=GOLD); self._clbl.pack(side="left",padx=(0,8))
        left=ctk.CTkScrollableFrame(self._gf,fg_color="transparent",corner_radius=0)
        left.grid(row=1,column=0,sticky="nsew",padx=(8,4),pady=(0,8))
        def gsec(t): ctk.CTkLabel(left,text=t,font=("Segoe UI",10,"bold"),text_color=GOLD).pack(anchor="w",padx=8,pady=(10,1))
        def grow(lbl,key,default,hint=""):
            r=ctk.CTkFrame(left,fg_color="transparent"); r.pack(fill="x",padx=8,pady=2)
            ctk.CTkLabel(r,text=lbl,font=FS,text_color=TEXT,width=185,anchor="w").pack(side="left")
            e=ctk.CTkEntry(r,font=FS,height=28,fg_color=CARD2,border_color=BORDER,text_color=TEXT)
            e.insert(0,default); e.pack(side="left",fill="x",expand=True)
            if hint: ctk.CTkLabel(r,text=hint,font=("Segoe UI",9),text_color=MUTED).pack(side="left",padx=(4,0))
            e.bind("<KeyRelease>",lambda _:self._upd_combos()); self._vars[key]=e
        gsec("\U0001f4c1  Data & Dates")
        grow("Data Path","g_data_path",r"C:\Users\Admin\Downloads\BreezeDownloader-v1.5.0\breeze_data")
        grow("From Date","g_from","2026-01-02"); grow("To Date","g_to","2026-04-21")
        gsec("\U0001f3af  ATM Selection")
        grow("Scan Starts","g_atm_start","09:16","HH:MM,..."); grow("Scan Ends","g_atm_end","09:21","HH:MM,...")
        grow("Max Prem Diffs","g_prem_diff","20.0","\u20b9,..."); grow("Hedge %s","g_hedge_pct","0.05","%,...")
        grow("Trail Steps (\u20b9)","g_trail_step","3.0","\u20b9,...")
        gsec("\U0001f4ca  VIX & SL")
        grow("VIX Thr%","g_vix_thr","3.0","%,..."); grow("SL% VIX<12","g_sl_lt12","0.35,0.40,0.45","...")
        grow("SL% 12-16 Calm","g_sl_calm","0.40","..."); grow("SL% 12-16 Vol","g_sl_vol","0.25","...")
        grow("SL% VIX 16-20","g_sl_1620","0.25","..."); grow("SL% VIX>20","g_sl_gt20","0.10,0.15,0.20","...")
        grow("SL Buffer (\u20b9)","g_sl_buffer","5.0","\u20b9,...")
        gsec("\U0001f4c8  ATR Trailing")
        grow("ATR Timeframes","g_atr_tf","5min,15min","tf,..."); grow("ATR Periods","g_atr_per","7,14,21","int,...")
        grow("ATR Multipliers","g_atr_mult","1.5,2.0","x,...")
        gsec("\U0001f6aa  Exit & Slippage")
        grow("EOD Times","g_eod","15:15,15:20,15:25","HH:MM,..."); grow("Slippage %s","g_slip","0.001","%,...")
        # Right panel
        right=ctk.CTkFrame(self._gf,fg_color="transparent"); right.grid(row=1,column=1,sticky="nsew",padx=(4,8),pady=(0,8))
        # Cores card
        cc=ctk.CTkFrame(right,fg_color=CARD2,corner_radius=8); cc.pack(fill="x",pady=(0,8))
        ctk.CTkLabel(cc,text="\U0001f5a5\ufe0f  CPU Cores for Grid Search",font=("Segoe UI",11,"bold"),text_color=BLUE).pack(anchor="w",padx=12,pady=(10,4))
        cr=ctk.CTkFrame(cc,fg_color="transparent"); cr.pack(fill="x",padx=12,pady=(0,6))
        ctk.CTkLabel(cr,text="Cores:",font=FS,text_color=TEXT,width=55).pack(side="left")
        self._cores=ctk.IntVar(value=max(1,MAX_CORES-1))
        ctk.CTkSlider(cr,from_=1,to=MAX_CORES,number_of_steps=max(1,MAX_CORES-1),
            variable=self._cores,command=self._on_cores,
            progress_color=GREEN,button_color=GREEN2,width=155).pack(side="left",padx=8)
        self._clbl2=ctk.CTkLabel(cr,text=f"{self._cores.get()} / {MAX_CORES}",
            font=("Segoe UI",12,"bold"),text_color=GOLD,width=65); self._clbl2.pack(side="left")
        ctk.CTkLabel(cc,
            text=f"Available: {MAX_CORES} logical cores\nRecommended: {max(1,MAX_CORES-1)} (leave 1 for OS)\n\nUses parallel threads \u2014 shares memory,\nno serialization overhead.\nNumpy releases GIL for real parallelism.",
            font=FS,text_color=MUTED,justify="left").pack(anchor="w",padx=12,pady=(0,10))
        # Info card
        ic=ctk.CTkFrame(right,fg_color=CARD2,corner_radius=8); ic.pack(fill="x")
        ctk.CTkLabel(ic,text="\u2139\ufe0f  Grid Search Info",font=("Segoe UI",11,"bold"),text_color=BLUE).pack(anchor="w",padx=12,pady=(10,4))
        ctk.CTkLabel(ic,
            text="Enter comma-separated values per param.\nAll combinations run in parallel.\n\nATM cache groups identical ATM params,\neliminating redundant strike lookups.\n\nKey names match StrategyParams exactly:\natr_timeframe, atr_period, atr_multiplier,\neod_exit_time, sl_pct_vix_lt12, etc.",
            font=FS,text_color=MUTED,justify="left").pack(anchor="w",padx=12,pady=(0,10))
        self._upd_combos()

    def _on_cores(self,val):
        v=int(float(val)); self._cores.set(v); self._clbl2.configure(text=f"{v} / {MAX_CORES}")

    def _upd_combos(self):
        try:
            total=1
            for k in ["g_atm_start","g_atm_end","g_prem_diff","g_hedge_pct","g_trail_step","g_vix_thr",
                      "g_sl_lt12","g_sl_calm","g_sl_vol","g_sl_1620","g_sl_gt20","g_sl_buffer",
                      "g_atr_tf","g_atr_per","g_atr_mult","g_eod","g_slip"]:
                v=self._vars.get(k)
                if v: total*=max(1,len([x.strip() for x in v.get().split(",") if x.strip()]))
            self._clbl.configure(text=f"{total:,}",text_color=RED if total>5000 else GOLD)
        except: self._clbl.configure(text="?",text_color=MUTED)

    def _build_results(self):
        self._rf=ctk.CTkFrame(self._main,fg_color=CARD,corner_radius=8)
        self._rf.grid(row=0,column=0,sticky="nsew"); self._rf.grid_remove()
        self._rf.columnconfigure(0,weight=1); self._rf.columnconfigure(1,weight=1); self._rf.rowconfigure(1,weight=1)
        ctk.CTkLabel(self._rf,text="  Results Summary",font=FH2,text_color=BLUE).grid(row=0,column=0,columnspan=2,sticky="w",padx=12,pady=10)
        self._mf=ctk.CTkScrollableFrame(self._rf,fg_color="transparent",corner_radius=0)
        self._mf.grid(row=1,column=0,sticky="nsew",padx=(8,4),pady=(0,8))
        right=ctk.CTkFrame(self._rf,fg_color="transparent"); right.grid(row=1,column=1,sticky="nsew",padx=(4,8),pady=(0,8))
        right.rowconfigure(1,weight=1); right.columnconfigure(0,weight=1)
        ctk.CTkLabel(right,text="Daily P&L",font=("Segoe UI",11,"bold"),text_color=GOLD).grid(row=0,column=0,sticky="w",padx=4,pady=(0,4))
        self._db=ctk.CTkTextbox(right,font=("Consolas",10),fg_color="#010409",text_color=TEXT,
            corner_radius=4,border_width=1,border_color=BORDER)
        self._db.grid(row=1,column=0,sticky="nsew")
        self._db.insert("end","Run a backtest to see daily P&L...\n"); self._db.configure(state="disabled")

    def populate(self,metrics,daily):
        for w in self._mf.winfo_children(): w.destroy()
        def card(lbl,val,col=TEXT):
            r=ctk.CTkFrame(self._mf,fg_color=CARD2,corner_radius=6); r.pack(fill="x",padx=4,pady=2)
            ctk.CTkLabel(r,text=lbl,font=FS,text_color=MUTED,width=200,anchor="w").pack(side="left",padx=10,pady=6)
            ctk.CTkLabel(r,text=str(val),font=("Segoe UI",11,"bold"),text_color=col).pack(side="right",padx=10)
        def sec(t): ctk.CTkLabel(self._mf,text=t,font=("Segoe UI",10,"bold"),text_color=GOLD).pack(anchor="w",padx=4,pady=(8,2))
        pnl=metrics.get("total_pnl",0); wr=metrics.get("win_rate_pct",0)
        sec("PERFORMANCE")
        card("Total P&L",f"\u20b9{pnl:,.2f}",GREEN if pnl>=0 else RED)
        card("Traded Days",metrics.get("traded_days",0))
        card("Win Rate",f"{wr:.1f}%",GREEN if wr>=50 else RED)
        card("Avg Daily P&L",f"\u20b9{metrics.get('avg_daily_pnl',0):,.2f}")
        card("Sharpe (Annual)",f"{metrics.get('sharpe',0):.3f}",GREEN if metrics.get("sharpe",0)>1 else TEXT)
        card("Std Dev (Daily)",f"\u20b9{metrics.get('std_daily_pnl',0):,.2f}")
        sec("RISK")
        card("Max Drawdown",f"\u20b9{metrics.get('max_drawdown',0):,.2f}",RED)
        card("Profit Factor",f"{metrics.get('profit_factor',0):.3f}")
        card("Risk : Reward",f"1 : {metrics.get('recovery_ratio',0):.2f}")
        sec("STREAKS & EXITS")
        card("Max Consec Profit",f"{metrics.get('max_consec_wins',0)} days",GREEN)
        card("Max Consec Loss",f"{metrics.get('max_consec_losses',0)} days",RED)
        card("Both Legs SL",metrics.get("both_legs_sl",0),RED)
        card("One Leg SL",metrics.get("one_leg_sl",0),GOLD)
        card("EOD Exits",metrics.get("eod_exits",0))
        self._mini["pnl"].configure(text=f"\u20b9{pnl:,.0f}",text_color=GREEN if pnl>=0 else RED)
        self._mini["wr"].configure(text=f"{wr:.1f}%")
        self._mini["sharpe"].configure(text=f"{metrics.get('sharpe',0):.3f}")
        self._mini["dd"].configure(text=f"\u20b9{metrics.get('max_drawdown',0):,.0f}",text_color=RED)
        self._mini["days"].configure(text=str(metrics.get("traded_days",0)))
        self._db.configure(state="normal"); self._db.delete("1.0","end")
        self._db.insert("end",f"{'Date':<12}{'ATM':>7}{'CE':>15}{'PE':>15}{'P&L':>13}{'Cumul':>13}\n")
        self._db.insert("end","\u2500"*75+"\n")
        cumul=0
        for r in daily:
            if r.get("status")!="ok":
                self._db.insert("end",f"{r.get('date',''):<12}  SKIP  {r.get('notes','')[:25]}\n"); continue
            cumul+=r.get("total_pnl",0)
            self._db.insert("end",f"{r.get('date',''):<12}{str(r.get('atm_strike','') or '-'):>7}"
                f"{str(r.get('ce_exit_reason','') or '-'):>15}{str(r.get('pe_exit_reason','') or '-'):>15}"
                f" \u20b9{r.get('total_pnl',0):>10,.0f} \u20b9{cumul:>10,.0f}\n")
        self._db.configure(state="disabled")

    def _browse(self):
        p=filedialog.askdirectory(title="Select breeze_data folder")
        if p: self._dp.delete(0,"end"); self._dp.insert(0,p)

    def _on_run(self):
        if self._running: return
        self._running=True; self._last_report=None; self._last_log=None
        self._open_btn.configure(state="disabled"); self._log_btn.configure(state="disabled")
        self._run_btn.configure(state="disabled",text="\u23f3  Running...")
        self._status.configure(text="\u25cf Running",text_color=GOLD)
        self._prog.set(0); self._pct.configure(text="0%"); self._clear_con()
        sys.stdout=QueueStream(self._q); sys.stderr=QueueStream(self._q)
        threading.Thread(target=self._worker,args=(self._collect(),),daemon=True).start()

    def _on_stop(self):
        self._running=False; self._q.put("\u26a0\ufe0f  Stop requested")

    def _collect(self):
        v=self._vars; g=lambda k: v[k].get()
        return {
            "mode":self._active_tab,
            "data_path":self._dp.get(),"from_date":self._frm.get(),"to_date":self._to.get(),
            "atm_scan_start":g("atm_scan_start"),"atm_scan_end":g("atm_scan_end"),
            "max_premium_diff":float(g("max_premium_diff")),"hedge_pct":float(g("hedge_pct")),
            "hedge_trail_step":float(g("hedge_trail_step")),"vix_intraday_threshold":float(g("vix_intraday_threshold")),
            "sl_pct_vix_lt12":float(g("sl_pct_vix_lt12")),"sl_pct_vix_12_16_calm":float(g("sl_pct_vix_12_16_calm")),
            "sl_pct_vix_12_16_volatile":float(g("sl_pct_vix_12_16_volatile")),
            "sl_pct_vix_16_20":float(g("sl_pct_vix_16_20")),"sl_pct_vix_gt20":float(g("sl_pct_vix_gt20")),
            "sl_buffer":float(g("sl_buffer")),"atr_timeframe":g("atr_timeframe"),
            "atr_period":int(g("atr_period")),"atr_multiplier":float(g("atr_multiplier")),
            "eod_exit_time":g("eod_exit_time"),"lot_size":int(g("lot_size")),
            "slippage_pct":float(g("slippage_pct")),"n_threads":int(self._cores.get()),
            "g_data_path":g("g_data_path"),"g_from":g("g_from"),"g_to":g("g_to"),
            "g_atm_start":g("g_atm_start"),"g_atm_end":g("g_atm_end"),"g_prem_diff":g("g_prem_diff"),
            "g_hedge_pct":g("g_hedge_pct"),"g_trail_step":g("g_trail_step"),"g_vix_thr":g("g_vix_thr"),
            "g_sl_lt12":g("g_sl_lt12"),"g_sl_calm":g("g_sl_calm"),"g_sl_vol":g("g_sl_vol"),
            "g_sl_1620":g("g_sl_1620"),"g_sl_gt20":g("g_sl_gt20"),"g_sl_buffer":g("g_sl_buffer"),
            "g_atr_tf":g("g_atr_tf"),"g_atr_per":g("g_atr_per"),"g_atr_mult":g("g_atr_mult"),
            "g_eod":g("g_eod"),"g_slip":g("g_slip"),
        }

    def _worker(self,cfg):
        try:
            from gui_runner import run_backtest
            result=run_backtest(cfg,progress_fn=self._set_prog,
                results_fn=lambda m,d: self.after(0,lambda:( self.populate(m,d), self._show("results"))))
            if result:
                self._last_report=result.get("report"); self._last_log=result.get("trade_log")
                if self._last_report: self.after(0,lambda: self._open_btn.configure(state="normal"))
                if self._last_log:    self.after(0,lambda: self._log_btn.configure(state="normal"))
        except Exception as ex:
            import traceback; print(f"Error: {ex}"); print(traceback.format_exc())
        finally:
            sys.stdout=self._orig_out; sys.stderr=self._orig_err
            self._running=False; self.after(0,self._done)

    def _set_prog(self,v):
        self.after(0,lambda: self._prog.set(v))
        self.after(0,lambda: self._pct.configure(text=f"{int(v*100)}%"))

    def _done(self):
        self._run_btn.configure(state="normal",text="\u25b6  RUN BACKTEST")
        self._status.configure(text="\u25cf Done",text_color=GREEN)
        self._prog.set(1); self._pct.configure(text="100%")

    def _poll(self):
        try:
            while True:
                msg=self._q.get_nowait()
                self._con.configure(state="normal"); self._con.insert("end",msg+"\n")
                self._con.see("end"); self._con.configure(state="disabled")
        except queue.Empty: pass
        self.after(80,self._poll)

    def _clear_con(self):
        self._con.configure(state="normal"); self._con.delete("1.0","end"); self._con.configure(state="disabled")

    def _open_report(self):
        if self._last_report and Path(self._last_report).exists(): os.startfile(self._last_report)

    def _open_log(self):
        if self._last_log and Path(self._last_log).exists(): os.startfile(self._last_log)

if __name__=="__main__":
    App().mainloop()
