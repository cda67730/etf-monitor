"""期貨籌碼：期交所「三大法人－區分各期貨契約」每日資料

資料來源：POST https://www.taifex.com.tw/cht/3/futContractsDateDown（Big5 CSV，可一次查一年，免金鑰）
每個商品、每種身份（自營商／投信／外資及陸資）一列，存交易淨額與未平倉多空口數、淨額。
頁面 /futures 畫出外資在 8 個期貨商品的「未平倉多空淨額（口數）」走勢（所有月份合計）。
"""
import csv
import datetime as dt
import io
import logging
import threading
import time

import requests
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)
URL = "https://www.taifex.com.tw/cht/3/futContractsDateDown"
OI_URL = "https://www.taifex.com.tw/cht/3/futDataDown"     # 每日行情（含各月份未沖銷契約數），一次最多約一個月
OI_BACKFILL_DAYS = 120
RETAIL = ("MTX", "小型臺指期貨")                            # 散戶多空比用小台
UA = {"User-Agent": "Mozilla/5.0 (etf-monitor futures)"}
BACKFILL_DAYS = 730      # 首次補兩年；資料只增不刪，頁面只畫近半年
INSTS = {"foreign": "外資及陸資", "trust": "投信", "dealer": "自營商"}
# 頁面上的 8 張圖（順序照畫面兩排）：(期交所商品名稱, 短名)
PRODUCTS = [
    ("股票期貨", "股票期貨"), ("金融期貨", "金融期貨"), ("美國那斯達克100期貨", "那斯達克100"), ("美國道瓊期貨", "道瓊"),
    ("臺股期貨", "台指期"), ("電子期貨", "電子期貨"), ("美國標普500期貨", "標普500"), ("美國費城半導體期貨", "費城半導體"),
]
_int = lambda s: int(str(s).replace(",", "").strip() or 0)
# 交易價差只算台指期（外資主要部位；小台、微台是散戶指標不算）：每點 200 元
SPREAD_PRODUCT, SPREAD_MULT = "臺股期貨", 200
TWSE = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK"   # 每月每日加權指數收盤


class FutStore:
    def __init__(self, db):
        self.db = db
        self.lock = threading.Lock()
        self._cache = {}
        self._q("""CREATE TABLE IF NOT EXISTS fut_inst (
            d TEXT NOT NULL, product TEXT NOT NULL, inst TEXT NOT NULL,
            net_trade INTEGER, long_oi INTEGER, short_oi INTEGER, net_oi INTEGER, net_oi_amt BIGINT,
            PRIMARY KEY (d, product, inst))""")
        self._q("""CREATE TABLE IF NOT EXISTS fut_trade (
            d TEXT NOT NULL, product TEXT NOT NULL, inst TEXT NOT NULL,
            long_trade INTEGER, long_amt BIGINT, short_trade INTEGER, short_amt BIGINT,
            PRIMARY KEY (d, product, inst))""")
        self._q("""CREATE TABLE IF NOT EXISTS taiex_close (d TEXT PRIMARY KEY, close REAL)""")
        self._q("""CREATE TABLE IF NOT EXISTS fut_oi (
            d TEXT NOT NULL, product TEXT NOT NULL, oi INTEGER, PRIMARY KEY (d, product))""")

    @property
    def ph(self):
        return "%s" if self.db.db_type == "postgresql" else "?"

    def _q(self, sql, params=(), fetch="none"):
        return self.db.execute_query(sql.replace("?", self.ph), params, fetch)

    # ---------- 抓取 ----------
    @staticmethod
    def fetch(start, end):
        """start/end: date；回傳 [(d, product, inst, net_trade, long_oi, short_oi, net_oi, net_oi_amt), ...]"""
        r = requests.post(URL, headers=UA, timeout=120, data={
            "queryStartDate": start.strftime("%Y/%m/%d"), "queryEndDate": end.strftime("%Y/%m/%d"), "commodityId": ""})
        r.raise_for_status()
        txt = r.content.decode("cp950", errors="replace")
        rows = list(csv.reader(io.StringIO(txt)))
        if not rows or rows[0][:3] != ["日期", "商品名稱", "身份別"]:
            raise ValueError(f"期交所回應格式不符：{txt[:120]!r}")
        h = rows[0]
        ix = {k: h.index(k) for k in ("多空交易口數淨額", "多方未平倉口數", "空方未平倉口數", "多空未平倉口數淨額", "多空未平倉契約金額淨額(千元)")}
        col = lambda key: next((i for i, c in enumerate(h) if c.replace(" ", "").startswith(key)), None)
        tx = [col("多方交易口數"), col("多方交易契約金額"), col("空方交易口數"), col("空方交易契約金額")]
        if None in tx:                                          # 欄位對不到就不存交易價差，其他照常
            logger.warning(f"[fut] 找不到多空交易口數／金額欄位：{h}")
            tx = None
        out = []
        for x in rows[1:]:
            if len(x) < len(h) or not x[0].strip():
                continue
            out.append((x[0].strip().replace("/", "-"), x[1].strip(), x[2].strip(), _int(x[ix["多空交易口數淨額"]]),
                        _int(x[ix["多方未平倉口數"]]), _int(x[ix["空方未平倉口數"]]), _int(x[ix["多空未平倉口數淨額"]]),
                        _int(x[ix["多空未平倉契約金額淨額(千元)"]])) + (tuple(_int(x[i]) for i in tx) if tx else ()))
        return out     # 每筆 12 欄：前 8 欄存 fut_inst，後 4 欄（多方／空方交易口數、契約金額千元）存 fut_trade

    def _save(self, rows):
        """批次寫入（首次補兩年約 1 萬多筆，逐筆開連線太慢）；同一鍵只留最後一筆，避免 ON CONFLICT 同批重複"""
        rows = list({r[:3]: r for r in rows}.values())
        if not rows:
            return
        trades = [r[:3] + r[8:12] for r in rows if len(r) >= 12 and r[1] == SPREAD_PRODUCT]
        rows = [r[:8] for r in rows]
        sql = ("INSERT INTO fut_inst (d, product, inst, net_trade, long_oi, short_oi, net_oi, net_oi_amt) VALUES {v} "
               "ON CONFLICT (d, product, inst) DO UPDATE SET net_trade = EXCLUDED.net_trade, long_oi = EXCLUDED.long_oi, "
               "short_oi = EXCLUDED.short_oi, net_oi = EXCLUDED.net_oi, net_oi_amt = EXCLUDED.net_oi_amt")
        with self.db.get_connection() as conn:
            cur = conn.cursor()
            if self.db.db_type == "postgresql":
                from psycopg2.extras import execute_values
                execute_values(cur, sql.format(v="%s"), rows, page_size=1000)
            else:
                cur.executemany(sql.format(v="(?, ?, ?, ?, ?, ?, ?, ?)"), rows)
                conn.commit()
        if trades:
            sql = ("INSERT INTO fut_trade (d, product, inst, long_trade, long_amt, short_trade, short_amt) VALUES {v} "
                   "ON CONFLICT (d, product, inst) DO UPDATE SET long_trade = EXCLUDED.long_trade, long_amt = EXCLUDED.long_amt, "
                   "short_trade = EXCLUDED.short_trade, short_amt = EXCLUDED.short_amt")
            with self.db.get_connection() as conn:
                cur = conn.cursor()
                if self.db.db_type == "postgresql":
                    from psycopg2.extras import execute_values
                    execute_values(cur, sql.format(v="%s"), trades, page_size=1000)
                else:
                    cur.executemany(sql.format(v="(?, ?, ?, ?, ?, ?, ?)"), trades)
                    conn.commit()

    # ---------- 交易價差（外資台指期）----------
    def backfill_trade(self):
        """fut_trade 是後來加的：第一次把 fut_inst 有的期間整段重抓一次（一次一年）"""
        r = self._q("SELECT MIN(d) AS a FROM fut_inst", fetch="one") or {}
        t = self._q("SELECT MIN(d) AS a FROM fut_trade", fetch="one") or {}
        if not r.get("a"):
            return 0
        first = dt.date.fromisoformat(r["a"])
        have = dt.date.fromisoformat(t["a"]) if t.get("a") else dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()
        if have <= first + dt.timedelta(days=7):                # 已經補齊
            return 0
        s, end, total = first, have, 0
        while s <= end:
            e = min(s + dt.timedelta(days=364), end)
            rows = self.fetch(s, e)
            self._save(rows)
            total += len(rows)
            s = e + dt.timedelta(days=1)
            time.sleep(1)
        self._cache.clear()
        return total

    def update_taiex(self, months=None):
        """加權指數每日收盤（證交所 FMTQIK，一次一個月）；預設補缺的月份到本月，第一次補兩年"""
        today = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()
        if months is None:
            last = (self._q("SELECT MAX(d) AS d FROM taiex_close", fetch="one") or {}).get("d")
            start = dt.date.fromisoformat(last).replace(day=1) if last else (today - dt.timedelta(days=BACKFILL_DAYS)).replace(day=1)
            months, m = [], start
            while m <= today:
                months.append(m)
                m = (m + dt.timedelta(days=32)).replace(day=1)
        n = 0
        for m in months:
            r = requests.get(TWSE, params={"date": m.strftime("%Y%m01"), "response": "json"}, headers=UA, timeout=40)
            r.raise_for_status()
            for row in r.json().get("data") or []:
                try:
                    y, mo, d = (int(x) for x in str(row[0]).split("/"))
                    day = dt.date(y + 1911, mo, d).isoformat()
                    close = float(str(row[4]).replace(",", ""))
                except (ValueError, IndexError):
                    continue
                self._q("INSERT INTO taiex_close (d, close) VALUES (?, ?) ON CONFLICT (d) DO UPDATE SET close = EXCLUDED.close", (day, close))
                n += 1
            time.sleep(1.5)
        return n

    def spread(self, inst="foreign", days=120):
        """台指期交易價差（點）＝買進均價 − 賣出均價；均價＝契約金額 ÷（口數 × 200）
        負值＝當天賣在高、買在低（先空後補，盤中主導下殺）；20 日合計看持續性。
        正負方向已用 2026-10 實際資料對照節目圖（大跌日 10/15 為負、7 月回檔時 20 日合計大幅為負）"""
        key = ("spread", inst, days)
        if key in self._cache:
            return self._cache[key]
        name = INSTS.get(inst, INSTS["foreign"])
        rows = self._q("SELECT d, long_trade, long_amt, short_trade, short_amt FROM fut_trade WHERE product = ? AND inst = ? "
                       "ORDER BY d DESC LIMIT ?", (SPREAD_PRODUCT, name, int(days) + 19), fetch="all") or []
        rows = rows[::-1]
        avg = lambda amt, n: amt * 1000 / (n * SPREAD_MULT) if n else None
        pts = []
        for r in rows:
            b, sl = avg(r["long_amt"], r["long_trade"]), avg(r["short_amt"], r["short_trade"])
            pts.append({"d": r["d"], "buy": b, "sell": sl, "spread": round(b - sl, 1) if b and sl else None})
        for i, p in enumerate(pts):                              # 20 日合計（不足 20 天不算）
            w = [x["spread"] for x in pts[max(0, i - 19):i + 1] if x["spread"] is not None]
            p["sum20"] = round(sum(w), 1) if i >= 19 else None
        pts = pts[-int(days):]
        tx = {r["d"]: r["close"] for r in (self._q("SELECT d, close FROM taiex_close WHERE d >= ?", (pts[0]["d"],), fetch="all") or [])} if pts else {}
        res = {"inst": inst, "inst_name": name, "product": SPREAD_PRODUCT, "dates": [p["d"] for p in pts],
               "spread": [p["spread"] for p in pts], "sum20": [p["sum20"] for p in pts], "taiex": [tx.get(p["d"]) for p in pts],
               "last": pts[-1] if pts else None}
        self._cache[key] = res
        return res

    @staticmethod
    def fetch_oi(cid, start, end):
        """全市場未平倉（一般交易時段各到期月份的未沖銷契約數加總）；回傳 {日期: 口數}"""
        r = requests.post(OI_URL, headers=UA, timeout=120, data={
            "down_type": "1", "commodity_id": cid, "commodity_id2": "",
            "queryStartDate": start.strftime("%Y/%m/%d"), "queryEndDate": end.strftime("%Y/%m/%d")})
        r.raise_for_status()
        rows = list(csv.reader(io.StringIO(r.content.decode("cp950", errors="replace"))))
        if not rows or rows[0][:2] != ["交易日期", "契約"]:
            raise ValueError("期交所行情回應格式不符（查詢區間可能太長）")
        h = rows[0]
        io_, it = h.index("未沖銷契約數"), h.index("交易時段")
        out = {}
        for x in rows[1:]:
            if len(x) > max(io_, it) and x[it].strip() == "一般" and x[io_].strip().isdigit():
                d = x[0].strip().replace("/", "-")
                out[d] = out.get(d, 0) + int(x[io_])
        return out

    def update_oi(self, today):
        cid, name = RETAIL
        r = self._q("SELECT MAX(d) AS d FROM fut_oi WHERE product = ?", (name,), fetch="one") or {}
        s = dt.date.fromisoformat(r["d"]) if r.get("d") else today - dt.timedelta(days=OI_BACKFILL_DAYS)
        n = 0
        while s <= today:                                       # 一次查一個月
            e = min(s + dt.timedelta(days=27), today)
            for d, oi in self.fetch_oi(cid, s, e).items():
                self._q("INSERT INTO fut_oi (d, product, oi) VALUES (?, ?, ?) ON CONFLICT (d, product) DO UPDATE SET oi = EXCLUDED.oi",
                        (d, name, oi))
                n += 1
            s = e + dt.timedelta(days=1)
            time.sleep(1)
        return n

    def retail(self):
        """小台散戶多空比：散戶多單−散戶空單＝−(三大法人小台多空淨額合計)；比例＝÷ 全市場未平倉"""
        name = RETAIL[1]
        rows = self._q("SELECT o.d AS d, o.oi AS oi, SUM(f.net_oi) AS inst FROM fut_oi o JOIN fut_inst f ON f.d = o.d AND f.product = o.product "
                       "WHERE o.product = ? GROUP BY o.d, o.oi ORDER BY o.d DESC LIMIT 2", (name,), fetch="all") or []
        if not rows or not rows[0]["oi"]:
            return None
        out = [{"d": r["d"], "oi": r["oi"], "net": -int(r["inst"]), "pct": round(-int(r["inst"]) / r["oi"] * 100, 2)} for r in rows if r["oi"]]
        cur = out[0]
        cur["chg"] = round(cur["pct"] - out[1]["pct"], 2) if len(out) > 1 else None
        return cur

    def retail_net(self, product="微型臺指期貨"):
        """散戶淨多空口數＝−(三大法人該商品多空淨額合計)；回傳 {d, net, chg}（chg＝較前一交易日）"""
        rows = self._q("SELECT d, SUM(net_oi) AS inst FROM fut_inst WHERE product = ? GROUP BY d ORDER BY d DESC LIMIT 2",
                       (product,), fetch="all") or []
        if not rows:
            return None
        cur = {"d": rows[0]["d"], "net": -int(rows[0]["inst"])}
        cur["chg"] = cur["net"] + int(rows[1]["inst"]) if len(rows) > 1 else None
        return cur

    def retail_series(self, product="微型臺指期貨", days=400):
        """市場情緒頁用：[(日期, 散戶淨口數, {})]"""
        rows = self._q("SELECT d, SUM(net_oi) AS inst FROM fut_inst WHERE product = ? GROUP BY d ORDER BY d DESC LIMIT ?",
                       (product, int(days)), fetch="all") or []
        return [(r["d"], -int(r["inst"]), {}) for r in reversed(rows)]

    def last_date(self):
        r = self._q("SELECT MAX(d) AS d FROM fut_inst", fetch="one") or {}
        return r.get("d")

    def backfill_older(self):
        """之前只補過約一年時，往前補到 BACKFILL_DAYS（放大圖可選兩年）"""
        r = self._q("SELECT MIN(d) AS d FROM fut_inst", fetch="one") or {}
        if not r.get("d"):
            return 0
        first = dt.date.fromisoformat(r["d"])
        target = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date() - dt.timedelta(days=BACKFILL_DAYS)
        total, s = 0, target
        while s < first - dt.timedelta(days=7):
            e = min(s + dt.timedelta(days=364), first - dt.timedelta(days=1))
            rows = self.fetch(s, e)
            self._save(rows)
            total += len(rows)
            s = e + dt.timedelta(days=1)
            time.sleep(1)
        if total:
            self._cache.clear()
        return total

    def update(self):
        """排程用：資料庫空的就補兩年，否則從最後一天補到今天（最後一天也重抓，避免盤後修正）"""
        with self.lock:
            today = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()
            last = self.last_date()
            start = dt.date.fromisoformat(last) if last else today - dt.timedelta(days=BACKFILL_DAYS)
            total, s = 0, start
            while s <= today:                                   # 一次最多抓一年
                e = min(s + dt.timedelta(days=364), today)
                rows = self.fetch(s, e)
                self._save(rows)
                total += len(rows)
                s = e + dt.timedelta(days=1)
                time.sleep(1)
            try:
                n_tx = self.update_taiex()
            except Exception as e:
                n_tx = f"失敗：{e}"
                logger.warning(f"[fut] 加權指數抓取失敗：{e}")
            try:
                n_oi = self.update_oi(today)
            except Exception as e:
                n_oi = f"失敗：{e}"
                logger.warning(f"[fut] 全市場未平倉抓取失敗：{e}")
            self._cache.clear()
            return f"寫入 {total} 筆，最新 {self.last_date()}；小台未平倉 {n_oi} 天；加權指數 {n_tx} 天"

    # ---------- 查詢 ----------
    def series(self, inst="foreign", days=120):
        key = (inst, days)
        if key in self._cache:
            return self._cache[key]
        name = INSTS.get(inst, INSTS["foreign"])
        dates = [r["d"] for r in (self._q("SELECT DISTINCT d FROM fut_inst ORDER BY d DESC LIMIT ?", (int(days),), fetch="all") or [])][::-1]
        if not dates:
            return {"inst": inst, "dates": [], "products": []}
        rows = self._q(f"SELECT d, product, net_oi, net_trade, long_oi, short_oi FROM fut_inst WHERE inst = ? AND d >= ? "
                       f"AND product IN ({', '.join(['?'] * len(PRODUCTS))})", (name, dates[0], *[p for p, _ in PRODUCTS]), fetch="all") or []
        by = {}
        for r in rows:
            by.setdefault(r["product"], {})[r["d"]] = r
        prods = []
        for p, short in PRODUCTS:
            m = by.get(p, {})
            vals = [m[d]["net_oi"] if d in m else None for d in dates]
            ok = [v for v in vals if v is not None]
            last = next((m[d] for d in reversed(dates) if d in m), None)
            chg = lambda n: (ok[-1] - ok[-1 - n]) if len(ok) > n else None
            prods.append({"product": p, "short": short, "values": vals,
                          "last": last and {"net_oi": last["net_oi"], "net_trade": last["net_trade"], "long_oi": last["long_oi"], "short_oi": last["short_oi"]},
                          "chg1": chg(1), "chg5": chg(5),
                          "pct": round((ok[-1] - min(ok)) / (max(ok) - min(ok)) * 100) if ok and max(ok) != min(ok) else None})
        res = {"inst": inst, "inst_name": name, "dates": dates, "products": prods}
        self._cache[key] = res
        return res


store = None


def init(db_config):
    global store
    if not db_config:
        return None
    try:
        store = FutStore(db_config)
    except Exception as e:
        logger.error(f"❌ 期貨籌碼資料表初始化失敗: {e}")
        return None
    no_oi = not (store._q("SELECT COUNT(*) AS n FROM fut_oi", fetch="one") or {}).get("n")
    def _first():
        try:
            if not store.last_date() or no_oi:
                logger.info("[fut] 首次補資料：" + store.update())
            n = store.backfill_older()
            if n:
                logger.info(f"[fut] 往前補到兩年：{n} 筆")
            n = store.backfill_trade()
            if n:
                logger.info(f"[fut] 補交易口數與金額（交易價差用）：{n} 筆")
            if not (store._q("SELECT COUNT(*) AS n FROM taiex_close", fetch="one") or {}).get("n"):
                logger.info(f"[fut] 補加權指數收盤：{store.update_taiex()} 天")
        except Exception as e:
            logger.error(f"[fut] 補資料失敗：{e}")
    threading.Thread(target=_first, daemon=True).start()
    return store


def create_router(templates):
    router = APIRouter()

    @router.get("/futures", response_class=HTMLResponse)
    async def futures_page(request: Request):
        return templates.TemplateResponse("futures.html", {"request": request, "products": PRODUCTS, "insts": INSTS})

    @router.get("/api/futures")
    async def futures_api(inst: str = Query("foreign"), days: int = Query(120, ge=20, le=750)):
        if not store:
            raise HTTPException(status_code=503, detail="資料庫無法使用")
        if inst not in INSTS:
            raise HTTPException(status_code=400, detail="inst 只能是 foreign、trust、dealer")
        return JSONResponse(await run_in_threadpool(store.series, inst, days))

    @router.get("/api/futures/spread")
    async def futures_spread(inst: str = Query("foreign"), days: int = Query(120, ge=20, le=750)):
        """台指期交易價差（買進均價−賣出均價，點）與 20 日合計、加權指數收盤"""
        if not store:
            raise HTTPException(status_code=503, detail="資料庫無法使用")
        if inst not in INSTS:
            raise HTTPException(status_code=400, detail="inst 只能是 foreign、trust、dealer")
        return JSONResponse(await run_in_threadpool(store.spread, inst, days))

    return router
