"""融資現償：證交所／櫃買中心每日融資融券餘額

- 資料來源（免金鑰，美國機房連得到）：
    上市 GET https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN?date=YYYYMMDD&selectType=ALL&response=json
         tables：「信用交易統計」（融資(交易單位)、融資金額(仟元) 的買進／賣出／現金償還／前日餘額／今日餘額）
                 「融資融券彙總」（個股：代號、名稱、融資買進、賣出、現金償還、前日餘額、今日餘額…）
    上櫃 GET https://www.tpex.org.tw/www/zh-tw/margin/balance?date=YYYY/MM/DD&response=json
         tables[0]（個股：代號、名稱、前資餘額、資買、資賣、現償、資餘額…），summary（合計(張)、融資金(仟元)）
  每天約 21:00 後公布
- 資料表：margin_daily（個股，只存 4 碼普通股／KY，不含 ETF）、margin_total（上市／上櫃合計）、margin_skip（休市日）
- 頁面 /margin：每日現償累加圖、融資大減／現償大增股票
"""
import datetime as dt
import logging
import threading
import time

import requests
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)
TZ = dt.timezone(dt.timedelta(hours=8))
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"}
TWSE = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
TPEX = "https://www.tpex.org.tw/www/zh-tw/margin/balance"
BACKFILL_DAYS = 400          # 第一次啟動補約一年多（日曆天）
PUBLISH_HOUR = 21            # 21:00 後才試抓當天
RATIO_FLOOR = 10             # 現償倍數的分母（前 20 日日均現償）至少以 10 張計，避免平常幾乎沒現償的冷門股倍數爆表


def _n(v):
    try:
        return int(str(v).replace(",", "").strip() or 0)
    except ValueError:
        return 0


def _stock(code):
    return len(code) == 4 and code.isdigit() and not code.startswith("00")


class MarginStore:
    def __init__(self, db):
        self.db = db
        self.lock = threading.Lock()
        self._q("""CREATE TABLE IF NOT EXISTS margin_daily (
            d TEXT NOT NULL, stock_id TEXT NOT NULL, name TEXT, market TEXT,
            buy INTEGER, sell INTEGER, repay INTEGER, prev_bal INTEGER, bal INTEGER, PRIMARY KEY (d, stock_id))""")
        self._q("""CREATE TABLE IF NOT EXISTS margin_total (
            d TEXT NOT NULL, market TEXT NOT NULL, repay INTEGER, repay_amt BIGINT, bal INTEGER, bal_amt BIGINT,
            PRIMARY KEY (d, market))""")
        self._q("CREATE TABLE IF NOT EXISTS margin_skip (d TEXT PRIMARY KEY)")

    def _q(self, sql, params=(), fetch="none"):
        ph = "%s" if self.db.db_type == "postgresql" else "?"
        return self.db.execute_query(sql.replace("?", ph), params, fetch)

    # ---------- 抓取 ----------
    @staticmethod
    def fetch_twse(day):
        j = requests.get(TWSE, params={"date": day.strftime("%Y%m%d"), "selectType": "ALL", "response": "json"},
                         headers=UA, timeout=30).json()
        if j.get("stat") != "OK":
            return None
        rows, total = [], None
        for t in j.get("tables") or []:
            title = t.get("title") or ""
            if "信用交易統計" in title:
                m = {r[0]: r for r in t.get("data") or []}
                lots, amt = m.get("融資(交易單位)"), m.get("融資金額(仟元)")
                if lots and amt:
                    total = (_n(lots[3]), _n(amt[3]), _n(lots[5]), _n(amt[5]))
            elif "融資融券彙總" in title:
                for r in t.get("data") or []:
                    code = r[0].strip()
                    if _stock(code):
                        rows.append((code, r[1].strip(), "上市", _n(r[2]), _n(r[3]), _n(r[4]), _n(r[5]), _n(r[6])))
        return (rows, total) if rows else None

    @staticmethod
    def fetch_tpex(day):
        j = requests.get(TPEX, params={"date": day.strftime("%Y/%m/%d"), "response": "json"}, headers=UA, timeout=30).json()
        t = (j.get("tables") or [{}])[0]
        data = t.get("data") or []
        if not data:
            return None
        rows = []
        for r in data:
            code = str(r[0]).strip()
            if _stock(code):
                rows.append((code, str(r[1]).strip(), "上櫃", _n(r[3]), _n(r[4]), _n(r[5]), _n(r[2]), _n(r[6])))
        total = None
        m = {str(r[1]).strip(): r for r in t.get("summary") or [] if len(r) > 6}
        lots, amt = m.get("合計(張)"), m.get("融資金(仟元)")
        if lots and amt:
            total = (_n(lots[5]), _n(amt[5]), _n(lots[6]), _n(amt[6]))
        return rows, total

    def _save(self, day, market, rows, total):
        d = day.isoformat()
        if self.db.db_type == "postgresql":
            from psycopg2.extras import execute_values
            with self.db.get_connection() as conn:
                cur = conn.cursor()
                execute_values(cur, "INSERT INTO margin_daily (d, stock_id, name, market, buy, sell, repay, prev_bal, bal) VALUES %s "
                                    "ON CONFLICT (d, stock_id) DO UPDATE SET name = EXCLUDED.name, market = EXCLUDED.market, buy = EXCLUDED.buy, "
                                    "sell = EXCLUDED.sell, repay = EXCLUDED.repay, prev_bal = EXCLUDED.prev_bal, bal = EXCLUDED.bal",
                               [(d, *r) for r in rows], page_size=1000)
                conn.commit()
        else:
            for r in rows:
                self._q("INSERT OR REPLACE INTO margin_daily (d, stock_id, name, market, buy, sell, repay, prev_bal, bal) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (d, *r))
        if total:
            self._q("INSERT INTO margin_total (d, market, repay, repay_amt, bal, bal_amt) VALUES (?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (d, market) DO UPDATE SET repay = EXCLUDED.repay, repay_amt = EXCLUDED.repay_amt, "
                    "bal = EXCLUDED.bal, bal_amt = EXCLUDED.bal_amt", (d, market, *total))

    def fetch_day(self, day):
        """抓一天；回 "ok"（至少一邊有資料）、"empty"（兩邊都明確回沒資料＝休市或還沒公布）、"error"（連線失敗，之後重試）"""
        got, err = False, False
        for market, fn in (("上市", self.fetch_twse), ("上櫃", self.fetch_tpex)):
            try:
                res = fn(day)
            except Exception as e:
                logger.warning(f"[margin] {day} {market} 抓取失敗：{e}")
                res, err = None, True
            if res:
                self._save(day, market, *res)
                got = True
            time.sleep(1.5)                     # 證交所每 5 秒約 3 次的限制
        return "ok" if got else ("error" if err else "empty")

    def update(self, backfill_days=None):
        """補到最新：從最後一天的隔天抓到今天（21:00 前不抓今天）；休市日記在 margin_skip 不重抓"""
        if not self.lock.acquire(blocking=False):
            return "另一個更新正在執行"
        try:
            now = dt.datetime.now(TZ)
            end = now.date() if now.hour >= PUBLISH_HOUR else now.date() - dt.timedelta(days=1)
            r = self._q("SELECT MAX(d) AS d FROM margin_total", fetch="one") or {}
            # 有資料時回頭檢查近 30 天（補之前連線失敗漏掉的日子），沒資料就補 BACKFILL_DAYS
            start = end - dt.timedelta(days=30 if r.get("d") else (backfill_days or BACKFILL_DAYS))
            skip = {x["d"] for x in self._q("SELECT d FROM margin_skip WHERE d >= ?", (start.isoformat(),), fetch="all") or []}
            have = {x["d"] for x in self._q("SELECT DISTINCT d FROM margin_total WHERE d >= ?", (start.isoformat(),), fetch="all") or []}
            n, day = 0, start
            while day <= end:
                ds = day.isoformat()
                if day.weekday() < 5 and ds not in skip and ds not in have:
                    st = self.fetch_day(day)
                    if st == "ok":
                        n += 1
                    elif st == "empty" and day < now.date():   # 今天沒資料可能只是還沒公布，不記休市
                        self._q("INSERT INTO margin_skip (d) VALUES (?) ON CONFLICT (d) DO NOTHING", (ds,))
                day += dt.timedelta(days=1)
            return f"融資融券寫入 {n} 天，最新 {self.last_date()}"
        finally:
            self.lock.release()

    def last_date(self):
        r = self._q("SELECT MAX(d) AS d FROM margin_total", fetch="one") or {}
        return r.get("d")

    # ---------- 查詢 ----------
    def dates(self, limit=60):
        rows = self._q("SELECT DISTINCT d FROM margin_total ORDER BY d DESC LIMIT ?", (int(limit),), fetch="all") or []
        return [r["d"] for r in rows]

    def totals(self, days=120):
        """全市場（上市＋上櫃）每日融資現償張數、金額（億元）與融資餘額"""
        rows = self._q("SELECT d, SUM(repay) AS repay, SUM(repay_amt) AS amt, SUM(bal) AS bal, SUM(bal_amt) AS bal_amt, COUNT(*) AS n "
                       "FROM margin_total GROUP BY d ORDER BY d DESC LIMIT ?", (int(days),), fetch="all") or []
        rows = rows[::-1]
        # PostgreSQL 的 SUM(BIGINT) 回 Decimal，先轉 float 才能和浮點數運算
        return {"dates": [r["d"] for r in rows], "repay": [int(r["repay"] or 0) for r in rows],
                "amt": [round(float(r["amt"] or 0) / 1e5, 2) for r in rows],
                "bal_amt": [round(float(r["bal_amt"] or 0) / 1e5, 1) for r in rows],
                "partial": [r["d"] for r in rows if r["n"] < 2]}

    def ranking(self, date=None, period=1, top=150):
        """融資大減、現償大增：期間 N 個交易日（含 date）
        chg＝期末餘額 − 期初前日餘額；repay＝期間現償合計；ratio＝期間日均現償 ÷ 期間前 20 日日均現償"""
        ds = self.dates(period + 25)
        if date:
            ds = [d for d in ds if d <= date]
        if len(ds) < period:
            return {"date": None, "down": [], "repay": []}
        win, prior = ds[:period], ds[period:period + 20]
        end, start = win[0], win[-1]
        rows = self._q(
            "SELECT stock_id, MAX(name) AS name, MAX(market) AS market, SUM(repay) AS repay, SUM(buy) AS buy, SUM(sell) AS sell, "
            "SUM(CASE WHEN d = ? THEN bal END) AS bal, SUM(CASE WHEN d = ? THEN prev_bal END) AS prev "
            "FROM margin_daily WHERE d >= ? AND d <= ? GROUP BY stock_id", (end, start, start, end), fetch="all") or []
        base = {}
        if prior:
            for r in self._q("SELECT stock_id, SUM(repay) AS s FROM margin_daily WHERE d >= ? AND d <= ? GROUP BY stock_id",
                             (prior[-1], prior[0]), fetch="all") or []:
                base[r["stock_id"]] = float(r["s"] or 0) / len(prior)
        out = []
        for r in rows:
            bal, prev = (int(r["bal"]) if r["bal"] is not None else None), (int(r["prev"]) if r["prev"] is not None else None)
            if bal is None or prev is None:
                continue
            chg = bal - prev
            avg = float(r["repay"] or 0) / period
            b = base.get(r["stock_id"], 0)
            out.append({"id": r["stock_id"], "name": r["name"], "market": r["market"], "bal": bal, "prev": prev, "chg": chg,
                        "chg_pct": round(chg / prev * 100, 2) if prev else None, "repay": int(r["repay"] or 0),
                        "repay_pct": round(int(r["repay"] or 0) / prev * 100, 2) if prev else None,
                        "ratio": round(avg / max(b, RATIO_FLOOR), 1), "buy": int(r["buy"] or 0), "sell": int(r["sell"] or 0)})
        # 融資大減：至少減 100 張，依減幅（%）排；現償大增：期間現償至少 50 張，依倍數排（沒有基期的放後面）
        down = sorted([x for x in out if x["chg"] <= -100 and x["chg_pct"] is not None], key=lambda x: x["chg_pct"])[:top]
        rep = sorted([x for x in out if x["repay"] >= 50 * period ** 0.5], key=lambda x: (-x["ratio"], -x["repay"]))[:top]
        return {"date": end, "start": start, "period": period, "down": down, "repay": rep}


store = None


def init(db_config):
    global store
    if not db_config:
        return None
    try:
        store = MarginStore(db_config)
    except Exception as e:
        logger.error(f"❌ 融資融券資料表初始化失敗: {e}")
        return None

    def bg():
        try:
            if not store.last_date():
                logger.info(f"[margin] 首次補資料：{store.update()}")
        except Exception as e:
            logger.error(f"[margin] 首次補資料失敗：{e}")
    threading.Thread(target=bg, daemon=True).start()
    return store


def create_router(templates):
    router = APIRouter()

    def need():
        if not store:
            raise HTTPException(status_code=503, detail="資料庫無法使用")
        return store

    @router.get("/margin", response_class=HTMLResponse)
    async def margin_page(request: Request):
        dates = await run_in_threadpool(need().dates, 20) if store else []
        return templates.TemplateResponse("margin.html", {"request": request, "dates": dates})

    @router.get("/api/margin/totals")
    async def margin_totals(days: int = Query(120, ge=5, le=750)):
        return JSONResponse(await run_in_threadpool(need().totals, days))

    @router.get("/api/margin/ranking")
    async def margin_ranking(date: str = Query(None), period: int = Query(1, ge=1, le=20)):
        return JSONResponse(await run_in_threadpool(need().ranking, date, period))

    return router
