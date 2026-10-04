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


class FutStore:
    def __init__(self, db):
        self.db = db
        self.lock = threading.Lock()
        self._cache = {}
        self._q("""CREATE TABLE IF NOT EXISTS fut_inst (
            d TEXT NOT NULL, product TEXT NOT NULL, inst TEXT NOT NULL,
            net_trade INTEGER, long_oi INTEGER, short_oi INTEGER, net_oi INTEGER, net_oi_amt BIGINT,
            PRIMARY KEY (d, product, inst))""")
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
        out = []
        for x in rows[1:]:
            if len(x) < len(h) or not x[0].strip():
                continue
            out.append((x[0].strip().replace("/", "-"), x[1].strip(), x[2].strip(), _int(x[ix["多空交易口數淨額"]]),
                        _int(x[ix["多方未平倉口數"]]), _int(x[ix["空方未平倉口數"]]), _int(x[ix["多空未平倉口數淨額"]]),
                        _int(x[ix["多空未平倉契約金額淨額(千元)"]])))
        return out

    def _save(self, rows):
        """批次寫入（首次補兩年約 1 萬多筆，逐筆開連線太慢）；同一鍵只留最後一筆，避免 ON CONFLICT 同批重複"""
        rows = list({r[:3]: r for r in rows}.values())
        if not rows:
            return
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

    def last_date(self):
        r = self._q("SELECT MAX(d) AS d FROM fut_inst", fetch="one") or {}
        return r.get("d")

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
                n_oi = self.update_oi(today)
            except Exception as e:
                n_oi = f"失敗：{e}"
                logger.warning(f"[fut] 全市場未平倉抓取失敗：{e}")
            self._cache.clear()
            return f"寫入 {total} 筆，最新 {self.last_date()}；小台未平倉 {n_oi} 天"

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
    if not store.last_date() or no_oi:
        def _first():
            try:
                logger.info("[fut] 首次補資料：" + store.update())
            except Exception as e:
                logger.error(f"[fut] 首次補資料失敗：{e}")
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

    return router
