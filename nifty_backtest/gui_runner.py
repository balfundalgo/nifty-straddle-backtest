"""
gui_runner.py - Bridges GUI to backtest engine.
ThreadPoolExecutor for parallelism (safe with Tkinter on Windows).
NOTE: ProcessPoolExecutor deadlocks with Tkinter on Windows due to IPC pipe
size limits with large data. Threads share memory - no serialization needed.
Numpy releases GIL so threads get real CPU parallelism on heavy computation.
"""
import sys, os, csv, time, threading
from datetime import datetime
from itertools import product, groupby
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def run_backtest(cfg, progress_fn, results_fn):
    from config import StrategyParams
    from data_loader import DataLoader, PathConfig
    from day_simulator import DaySimulator
    from metrics import compute_metrics, rank_param_sets
    from report import generate_report
    import pandas as pd

    mode=cfg["mode"]; ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    data_path=cfg.get("g_data_path") if mode=="grid" else cfg.get("data_path")
    from_date=cfg.get("g_from")      if mode=="grid" else cfg.get("from_date")
    to_date  =cfg.get("g_to")        if mode=="grid" else cfg.get("to_date")

    print(f"{'='*55}")
    print(f"  Mode  : {'Single Run' if mode=='single' else 'Grid Search'}")
    print(f"  Range : {from_date} to {to_date}")
    print(f"{'='*55}")

    loader=DataLoader(PathConfig(base_path=data_path))
    loaded=loader.preload_all(from_date,to_date,log_fn=lambda m:print(m),progress_fn=progress_fn)
    if not loaded:
        print(f"No valid data found for {from_date} to {to_date}"); return None
    progress_fn(0.1)

    if mode=="single":
        params=_make_params(cfg)
        print(f"  Params: {params}")
        sim=DaySimulator(params); results=[]; total=len(loaded)
        for i,(date_str,day) in enumerate(loaded):
            res=sim.simulate(day)
            pnl=f"{res.total_pnl:>10,.2f}" if res.status=="ok" else "  (skipped)  "
            note=f"  [{res.notes}]" if res.notes else ""
            print(f"  {date_str}  {'OK' if res.status=='ok' else 'SKIP'}  PnL={pnl}  ATM={res.atm_strike or '-':>6}  CE={res.ce_exit_reason or '-':>12}  PE={res.pe_exit_reason or '-':>12}{note}")
            results.append(res); progress_fn(0.1+0.8*(i+1)/total)
        m=compute_metrics(results,params.to_dict()); _print_m(m)
        rp=f"single_run_{from_date}_{to_date}.xlsx"; lp=f"trade_log_{ts}.csv"
        generate_report(pd.DataFrame([m]),results,output_path=rp); _write_log(results,lp)
        results_fn(m,[r.to_dict() for r in results]); progress_fn(1.0)
        return {"report":rp,"trade_log":lp}

    elif mode=="grid":
        def ps(k): return [x.strip() for x in cfg[k].split(",") if x.strip()]
        def pi(k): return [int(x.strip()) for x in cfg[k].split(",") if x.strip()]
        def pf(k): return [float(x.strip()) for x in cfg[k].split(",") if x.strip()]
        GRID={
            "atm_scan_start":ps("g_atm_start"),"atm_scan_end":ps("g_atm_end"),
            "max_premium_diff":pf("g_prem_diff"),"hedge_pct":pf("g_hedge_pct"),
            "hedge_trail_step":pf("g_trail_step"),"vix_intraday_threshold":pf("g_vix_thr"),
            "sl_pct_vix_lt12":pf("g_sl_lt12"),"sl_pct_vix_12_16_calm":pf("g_sl_calm"),
            "sl_pct_vix_12_16_volatile":pf("g_sl_vol"),"sl_pct_vix_16_20":pf("g_sl_1620"),
            "sl_pct_vix_gt20":pf("g_sl_gt20"),"sl_buffer":pf("g_sl_buffer"),
            "atr_timeframe":ps("g_atr_tf"),"atr_period":pi("g_atr_per"),
            "atr_multiplier":pf("g_atr_mult"),"eod_exit_time":ps("g_eod"),"slippage_pct":pf("g_slip"),
        }
        keys,vals=list(GRID.keys()),list(GRID.values())
        combos=[]
        for v in product(*vals):
            p=_make_params(cfg)
            for k,vv in zip(keys,v): setattr(p,k,vv)
            combos.append(p)
        total=len(combos); n_days=len(loaded)
        print(f"Grid: {total:,} combos x {n_days} days = {total*n_days:,} sims")

        def atm_key(p): return (p.atm_scan_start,p.atm_scan_end,p.max_premium_diff,p.hedge_pct,p.slippage_pct)
        sorted_combos=sorted(combos,key=atm_key)
        atm_groups={k:list(v) for k,v in groupby(sorted_combos,key=atm_key)}
        n_groups=len(atm_groups)
        print(f"ATM cache: {n_groups} groups x {n_days} days = {n_groups*n_days} lookups (was {total*n_days:,})")

        atm_cache={}
        for gi,(key,grp) in enumerate(atm_groups.items()):
            tmp=DaySimulator(grp[0]); entries=[]
            for _,day in loaded:
                try: e=tmp.compute_day_entry(day)
                except: e=None
                entries.append(e)
            atm_cache[key]=entries
            if (gi+1)%max(1,n_groups//5)==0 or gi==n_groups-1:
                print(f"  Cache [{gi+1}/{n_groups}] built")
        print("ATM cache ready"); progress_fn(0.15)

        n_threads=int(cfg.get("n_threads",4)); lock=threading.Lock()
        print(f"Running on {n_threads} parallel threads")
        metrics_list=[None]*total; completed=[0]; t0=time.time()

        def run_one(args):
            i,p,entries=args; sim=DaySimulator(p); results=[]
            for j,(_,day) in enumerate(loaded):
                e=entries[j]
                results.append(sim.simulate_with_entry(day,e) if e is not None else sim.simulate(day))
            m=compute_metrics(results,p.to_dict()); m["combo_idx"]=i+1; return i,m,p

        work=[(i,p,atm_cache[atm_key(p)]) for i,p in enumerate(combos)]
        with ThreadPoolExecutor(max_workers=n_threads) as ex:
            futures={ex.submit(run_one,w):w[0] for w in work}
            for future in as_completed(futures):
                i,m,p=future.result(); metrics_list[i]=m; completed[0]+=1; c=completed[0]
                elapsed=time.time()-t0; eta=elapsed/c*(total-c) if c>0 else 0
                progress_fn(0.15+0.75*c/total)
                with lock:
                    print(f"  [{c:>4}/{total}]  PnL={m['total_pnl']:>10,.0f}  WR={m['win_rate_pct']:>5.1f}%  Sharpe={m['sharpe']:>6.3f}  ATR[{p.atr_timeframe},p{p.atr_period},x{p.atr_multiplier}]  EOD={p.eod_exit_time}  ETA={eta/60:.1f}min")

        metrics_list=[m for m in metrics_list if m is not None]
        ranked=rank_param_sets(metrics_list)
        print("\nTOP 10 COMBINATIONS:")
        for _,row in ranked.head(10).iterrows():
            print(f"  #{int(row['rank']):<3}  PnL={row['total_pnl']:>10,.0f}  WR={row['win_rate_pct']:>5.1f}%  Sharpe={row['sharpe']:>6.3f}  ATR[{row.get('atr_timeframe','?')},p{row.get('atr_period','?')},x{row.get('atr_multiplier','?')}]  EOD={row.get('eod_exit_time','?')}")

        best=ranked.iloc[0]; best_p=_make_params(cfg)
        for col in best_p.to_dict().keys():
            if col in best: setattr(best_p,col,best[col])
        sim=DaySimulator(best_p); bc=atm_cache.get(atm_key(best_p),[None]*n_days)
        best_daily=[sim.simulate_with_entry(day,bc[j]) if bc[j] is not None else sim.simulate(day)
                    for j,(_,day) in enumerate(loaded)]
        best_m=compute_metrics(best_daily,best_p.to_dict()); _print_m(best_m)
        results_fn(best_m,[r.to_dict() for r in best_daily])
        rp=f"grid_search_{from_date}_{to_date}.xlsx"; lp=f"trade_log_best_{ts}.csv"
        generate_report(ranked,best_daily,output_path=rp); _write_log(best_daily,lp)
        progress_fn(1.0); return {"report":rp,"trade_log":lp}
    return None


def _make_params(cfg):
    from config import StrategyParams
    return StrategyParams(
        atm_scan_start=cfg.get("atm_scan_start","09:16"),atm_scan_end=cfg.get("atm_scan_end","09:21"),
        max_premium_diff=float(cfg.get("max_premium_diff",20.0)),hedge_pct=float(cfg.get("hedge_pct",0.05)),
        hedge_trail_step=float(cfg.get("hedge_trail_step",3.0)),vix_intraday_threshold=float(cfg.get("vix_intraday_threshold",3.0)),
        sl_pct_vix_lt12=float(cfg.get("sl_pct_vix_lt12",0.40)),sl_pct_vix_12_16_calm=float(cfg.get("sl_pct_vix_12_16_calm",0.40)),
        sl_pct_vix_12_16_volatile=float(cfg.get("sl_pct_vix_12_16_volatile",0.25)),sl_pct_vix_16_20=float(cfg.get("sl_pct_vix_16_20",0.25)),
        sl_pct_vix_gt20=float(cfg.get("sl_pct_vix_gt20",0.15)),sl_buffer=float(cfg.get("sl_buffer",5.0)),
        atr_timeframe=cfg.get("atr_timeframe","5min"),atr_period=int(cfg.get("atr_period",14)),
        atr_multiplier=float(cfg.get("atr_multiplier",1.5)),eod_exit_time=cfg.get("eod_exit_time","15:20"),
        lot_size=int(cfg.get("lot_size",75)),slippage_pct=float(cfg.get("slippage_pct",0.001)),
    )

def _print_m(m):
    print(f"  Total P&L: {m.get('total_pnl',0):>12,.2f}")
    print(f"  Win rate:  {m.get('win_rate_pct',0):.1f}%  Sharpe: {m.get('sharpe',0):.3f}")
    print(f"  Max DD:    {m.get('max_drawdown',0):>12,.2f}  PF: {m.get('profit_factor',0):.3f}")

def _write_log(results,path):
    fields=["date","expiry","status","entry_time","atm_strike","ce_entry","ce_sl","ce_exit",
            "ce_exit_reason","ce_exit_time","pe_entry","pe_sl","pe_exit","pe_exit_reason","pe_exit_time",
            "ce_hedge_strike","ce_hedge_entry","ce_hedge_exit","ce_hedge_exit_reason",
            "pe_hedge_strike","pe_hedge_entry","pe_hedge_exit","pe_hedge_exit_reason",
            "vix_at_entry","total_pnl","notes"]
    with open(path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore"); w.writeheader()
        for r in results: w.writerow({k:getattr(r,k,"") for k in fields})
    print(f"Trade log saved: {path}")
