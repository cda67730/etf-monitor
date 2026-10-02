# -*- coding: utf-8 -*-
"""三大法人同買＋連續買超天數（資料存 PostgreSQL）

資料表
  inst_daily       每日每檔三大法人買賣超（單位：股）
  inst_no_trading  已確認的非交易日

路由
  GET /inst-cobuy                     頁面（INST_COBUY_PUBLIC=true 時免登入）
  GET /api/inst-cobuy/dates           可切換日期與每日同買檔數
  GET /api/inst-cobuy?date=YYYYMMDD   指定日期的計算結果
  GET /api/inst-cobuy.csv?date=...    下載 CSV

排程：scheduler.py 的 "inst" 工作（預設平日 18:20，SCHED_INST 可改）
定義：張＝股數/1000 四捨五入，≥1 張才算買超；只含 4 碼普通股／KY；連買最多回溯 N 天
"""
import csv
import datetime as dt
import glob
import io
import logging
import os
import re
import threading
import time

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

from inst_cobuy.fetch import twse, tpex, SLEEP, TW

logger = logging.getLogger(__name__)

N = int(os.getenv("INST_STREAK_DAYS", "40"))      # 連買最多回溯天數／統計區間
KEEP = 20                                         # 頁面可切換的日期數
BACKFILL = N + KEEP                               # 資料庫要保有的交易日數
PUBLIC = os.getenv("INST_COBUY_PUBLIC", "true").lower() == "true"   # 預設公開；設 false 改為需登入
SEED_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "inst_cobuy", "data")
KINDS = ("foreign", "trust", "dealer")
PERIODS = (5, 10)                                     # 頁面「近 5 日／近 10 日」篩選
STOCK_RE = re.compile(r"[1-9]\d{3}")


class InstFlow:
    def __init__(self, db_config):
        self.db = db_config
        self.lock = threading.Lock()
        self._mem = None          # 最近 BACKFILL 天的資料（記憶體快取）
        self._results = {}
        self._init_tables()

    # ---------- DB ----------
    @property
    def pg(self):
        return self.db.db_type == "postgresql"

    def _ph(self, n=1):
        return ", ".join(["%s" if self.pg else "?"] * n)

    def _run(self, sql, params=(), fetch=False, many=None):
        with self.db.get_connection() as conn:
            cur = conn.cursor()
            if many is not None:
                if self.pg:
                    from psycopg2.extras import execute_values
                    execute_values(cur, re.sub(r"VALUES \([^)]*\)", "VALUES %s", sql, count=1), many, page_size=1000)
                else:
                    cur.executemany(sql, many)
            else:
                cur.execute(sql, params)
            rows = cur.fetchall() if fetch else None
            if not self.pg:
                conn.commit()
            if rows is None:
                return None
            return [tuple(r.values()) if isinstance(r, dict) else tuple(r) for r in rows]

    def _init_tables(self):
        self._run("""CREATE TABLE IF NOT EXISTS inst_daily (
            trade_date TEXT NOT NULL, stock_id TEXT NOT NULL, name TEXT, market TEXT,
            foreign_net BIGINT, trust_net BIGINT, dealer_net BIGINT,
            PRIMARY KEY (trade_date, stock_id))""")
        self._run("CREATE INDEX IF NOT EXISTS idx_inst_daily_date ON inst_daily(trade_date)")
        self._run("CREATE TABLE IF NOT EXISTS inst_no_trading (trade_date TEXT PRIMARY KEY)")

    def dates_in_db(self):
        return [r[0] for r in self._run("SELECT DISTINCT trade_date FROM inst_daily ORDER BY trade_date", fetch=True)]

    def _mark_no_trading(self, key):
        self._run(f"INSERT INTO inst_no_trading (trade_date) VALUES ({self._ph()}) ON CONFLICT DO NOTHING", (key,))

    def _save_day(self, key, rows):
        sql = (f"INSERT INTO inst_daily (trade_date, stock_id, name, market, foreign_net, trust_net, dealer_net) "
               f"VALUES ({self._ph(7)}) ON CONFLICT (trade_date, stock_id) DO UPDATE SET "
               f"name=excluded.name, market=excluded.market, foreign_net=excluded.foreign_net, "
               f"trust_net=excluded.trust_net, dealer_net=excluded.dealer_net")
        self._run(sql, many=[(key, *r) for r in rows])
        self._invalidate()

    def _invalidate(self):
        self._mem = None
        self._results = {}

    # ---------- 匯入與抓取 ----------
    def import_seed(self):
        """把 repo 的 inst_cobuy/data/*.csv 匯入資料庫（只補資料庫沒有的日期）"""
        have = set(self.dates_in_db())
        n = 0
        for f in sorted(glob.glob(os.path.join(SEED_DIR, "[0-9]" * 8 + ".csv"))):
            key = os.path.basename(f)[:8]
            if key in have:
                continue
            with open(f, encoding="utf-8") as fp:
                rows = [(r["stock_id"], r["name"], r["market"], int(float(r["foreign"])),
                         int(float(r["trust"])), int(float(r["dealer"]))) for r in csv.DictReader(fp)]
            self._save_day(key, rows)
            n += 1
        nt = os.path.join(SEED_DIR, "_no_trading.txt")
        if os.path.exists(nt):
            for k in open(nt).read().split():
                self._mark_no_trading(k)
        if n:
            logger.info(f"[inst] 從 repo 匯入 {n} 天歷史資料")
        return n

    def scrape(self, want=BACKFILL):
        """補齊最近 want 個交易日；回傳摘要字串，全部失敗時丟出例外（排程會記為失敗）"""
        with self.lock:
            have = set(self.dates_in_db())
            no_trade = {r[0] for r in self._run("SELECT trade_date FROM inst_no_trading", fetch=True)}
            now = dt.datetime.now(TW)
            day = now.date() if now.hour >= 17 else now.date() - dt.timedelta(days=1)   # 17:00 後才抓當天
            seen = tried = fails = 0
            added, waiting, errors = [], [], []
            while seen < want and tried < want * 2 + 30:
                tried += 1
                key = day.strftime("%Y%m%d")
                if day.weekday() >= 5 or key in no_trade:
                    day -= dt.timedelta(days=1); continue
                if key in have:
                    seen += 1; day -= dt.timedelta(days=1); continue
                try:
                    a = twse(day)
                except Exception as e:
                    errors.append(f"{key} 證交所：{e}"); fails += 1
                    logger.error(f"[inst] {key} 證交所失敗：{e}")
                    if fails >= 3:
                        break
                    day -= dt.timedelta(days=1); continue
                time.sleep(SLEEP)
                if a is None:
                    logger.info(f"[inst] {key} 無資料（休市或尚未公布）")
                    if day < now.date():
                        self._mark_no_trading(key)
                    else:
                        waiting.append(key)
                    day -= dt.timedelta(days=1); continue
                try:
                    b = tpex(day) or []
                except Exception as e:
                    logger.error(f"[inst] {key} 櫃買失敗：{e}"); b = []
                time.sleep(SLEEP)
                if not b and (now.date() - day).days <= 3:
                    logger.warning(f"[inst] {key} 櫃買尚未公布，下次再補")
                    waiting.append(key)
                    day -= dt.timedelta(days=1); continue
                self._save_day(key, a + b)
                logger.info(f"[inst] {key} 上市 {len(a)} 檔、上櫃 {len(b)} 檔")
                added.append(key)
                seen += 1
                day -= dt.timedelta(days=1)
            if errors and not added:
                raise RuntimeError("；".join(errors[:3]))
            latest = (self.dates_in_db() or ["—"])[-1]
            msg = f"新增 {len(added)} 天，最新 {latest}"
            if waiting:
                msg += f"（{'、'.join(waiting)} 尚未公布）"
            return msg

    # ---------- 計算 ----------
    def _load(self):
        """一次讀入最近 BACKFILL 天，轉成每檔股票的逐日陣列（張）"""
        if self._mem is not None:
            return self._mem
        dates = self.dates_in_db()[-BACKFILL:]
        S = {}
        if dates:
            rows = self._run(
                f"SELECT trade_date, stock_id, name, market, foreign_net, trust_net, dealer_net "
                f"FROM inst_daily WHERE trade_date >= {self._ph()} ORDER BY trade_date", (dates[0],), fetch=True)
            idx = {d: i for i, d in enumerate(dates)}
            for d, sid, name, mkt, f, t, de in rows:
                if not STOCK_RE.fullmatch(sid or "") or d not in idx:
                    continue
                s = S.setdefault(sid, {"name": name, "market": mkt, "names": {},
                                       "v": {k: [None] * len(dates) for k in KINDS}})
                s["names"][d] = (name, mkt)
                for k, v in zip(KINDS, (f, t, de)):
                    s["v"][k][idx[d]] = round((v or 0) / 1000)     # 張，零股不計
        self._mem = (dates, S)
        return self._mem

    @staticmethod
    def _streak(vals):
        n = 0
        for v in reversed(vals):
            if v is not None and v > 0:
                n += 1
            else:
                break
        return n

    def compute(self, last):
        if last in self._results:
            return self._results[last]
        dates, S = self._load()
        if last not in dates:
            return None
        end = dates.index(last) + 1
        start = max(0, end - N)
        win = dates[start:end]
        out = []
        for sid, s in S.items():
            V = {k: s["v"][k][start:end] for k in KINDS}
            today = {k: V[k][-1] for k in KINDS}
            pos = lambda k, i: V[k][i] is not None and V[k][i] > 0
            co = [all(pos(k, i) for k in KINDS) for i in range(len(win))]
            ft = [pos("foreign", i) and pos("trust", i) for i in range(len(win))]
            recent = range(max(0, len(win) - PERIODS[-1]), len(win))
            if not any(pos(k, i) for k in KINDS for i in recent):
                continue                                           # 近 10 日內至少一個法人買超過
            name, mkt = s["names"].get(last) or (s["name"], s["market"])
            r = {"id": sid, "name": name, "market": mkt}
            for k in KINDS:
                r[k] = today[k]
                r[k + "_streak"] = self._streak(V[k])
                r[k + "_sum"] = sum(x for x in V[k] if x is not None)
                r[k + "_days"] = sum(1 for x in V[k] if x is not None and x > 0)
            r["co_streak"] = self._streak([1 if c else 0 for c in co])
            r["co_days"] = sum(co)
            r["cobuy"] = co[-1]
            r["total"] = sum((today[k] or 0) for k in KINDS)
            # 外資＋投信同買（不管自營商）
            r["ft_cobuy"] = ft[-1]
            r["ft_streak"] = self._streak([1 if c else 0 for c in ft])
            r["ft_days"] = sum(ft)
            r["ft_total"] = (today["foreign"] or 0) + (today["trust"] or 0)
            # 土洋對作：1＝投信買外資賣，-1＝外資買投信賣；duel_streak＝同方向連續天數
            duel = [(1 if (t or 0) > 0 and (f or 0) < 0 else -1 if (f or 0) > 0 and (t or 0) < 0 else 0)
                    for f, t in zip(V["foreign"], V["trust"])]
            r["duel"] = duel[-1]
            r["duel_streak"] = self._streak([1 if (d == duel[-1] and d) else 0 for d in duel])
            # 近 5／10 日：同買天數、同買日的張數合計、各法人淨買賣合計
            for w in PERIODS:
                idx = range(max(0, len(win) - w), len(win))
                val = lambda k, i: V[k][i] or 0
                r[f"co_d{w}"] = sum(co[i] for i in idx)
                r[f"co_s{w}"] = sum(val("foreign", i) + val("trust", i) + val("dealer", i) for i in idx if co[i])
                r[f"ft_d{w}"] = sum(ft[i] for i in idx)
                r[f"ft_s{w}"] = sum(val("foreign", i) + val("trust", i) for i in idx if ft[i])
                for k, z in (("foreign", "f"), ("trust", "t"), ("dealer", "dl")):
                    r[f"{z}{w}"] = sum(val(k, i) for i in idx)
                    r[f"{k}_d{w}"] = sum(1 for i in idx if pos(k, i))          # 期間內買超天數
                # 期間土洋對作：以期間合計判斷方向，並算當天也是這個方向的天數
                dw = 1 if r[f"t{w}"] > 0 and r[f"f{w}"] < 0 else -1 if r[f"f{w}"] > 0 and r[f"t{w}"] < 0 else 0
                r[f"duel{w}"] = dw
                r[f"duel_d{w}"] = sum(1 for i in idx if dw and duel[i] == dw)
            out.append(r)
        res = {"date": last, "window": [win[0], win[-1]], "n_days": len(win), "max_days": N, "rows": out}
        self._results[last] = res
        return res

    def history(self):
        if "_hist" in self._results:
            return self._results["_hist"]
        dates, _ = self._load()
        hist = []
        for d in reversed(dates[-KEEP:]):
            co = sorted([x for x in self.compute(d)["rows"] if x["cobuy"]], key=lambda x: -x["total"])
            hist.append({"date": d, "count": len(co), "top": [f'{x["id"]} {x["name"]}' for x in co[:15]]})
        res = {"dates": [h["date"] for h in hist], "cobuy_history": hist, "max_days": N}
        self._results["_hist"] = res
        return res


flow = None


def init(db_config):
    """建表，並在背景匯入 repo 內的歷史 CSV（不拖慢啟動）"""
    global flow
    if not db_config:
        return None
    try:
        flow = InstFlow(db_config)
    except Exception as e:
        logger.error(f"❌ 三大法人資料表初始化失敗: {e}")
        return None

    def _seed():
        try:
            with flow.lock:
                flow.import_seed()
        except Exception as e:
            logger.error(f"❌ 三大法人歷史資料匯入失敗: {e}")
    threading.Thread(target=_seed, daemon=True).start()
    return flow


def create_router(templates, check_authentication):
    router = APIRouter()

    async def allowed(request):
        return PUBLIC or await check_authentication(request)

    def need_flow():
        if not flow:
            raise HTTPException(status_code=503, detail="資料庫無法使用")
        return flow

    async def latest_or(date):
        if date:
            return date
        ds = await run_in_threadpool(need_flow().dates_in_db)
        if not ds:
            raise HTTPException(status_code=404, detail="尚無資料")
        return ds[-1]

    @router.get("/inst-cobuy", response_class=HTMLResponse)
    async def inst_cobuy_page(request: Request):
        if not await allowed(request):
            return RedirectResponse(url="/login", status_code=302)
        return templates.TemplateResponse("inst_cobuy.html", {"request": request, "max_days": N})

    @router.get("/api/inst-cobuy/dates")
    async def inst_dates(request: Request):
        if not await allowed(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        return await run_in_threadpool(need_flow().history)

    @router.get("/api/inst-cobuy")
    async def inst_day(request: Request, date: str = Query(None)):
        if not await allowed(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        date = await latest_or(date)
        r = await run_in_threadpool(need_flow().compute, date)
        if not r:
            raise HTTPException(status_code=404, detail=f"查無 {date} 資料")
        return JSONResponse(r)

    @router.get("/api/inst-cobuy.csv")
    async def inst_csv(request: Request, date: str = Query(None)):
        if not await allowed(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        date = await latest_or(date)
        r = await run_in_threadpool(need_flow().compute, date)
        if not r:
            raise HTTPException(status_code=404, detail=f"查無 {date} 資料")
        n = r["n_days"]
        cols = [("id", "代號"), ("name", "名稱"), ("market", "市場"), ("cobuy", "三大同買"), ("total", "三大合計(張)"),
                ("co_streak", "同買連續天數"), ("co_days", f"{n}日內同買天數")]
        for k, z in (("foreign", "外資"), ("trust", "投信"), ("dealer", "自營商")):
            cols += [(k, f"{z}買超(張)"), (k + "_streak", f"{z}連買天數"),
                     (k + "_days", f"{z}{n}日買超天數"), (k + "_sum", f"{z}{n}日累計(張)")]
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow([c[1] for c in cols])
        for x in sorted(r["rows"], key=lambda x: (not x["cobuy"], -x["total"])):
            w.writerow(["Y" if x[c] is True else "" if x[c] is False else x[c] for c, _ in cols])
        return StreamingResponse(iter(["﻿" + buf.getvalue()]), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename=inst_cobuy_{date}.csv"})

    return router
