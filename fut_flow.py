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
# 盤中：期交所行情資訊網一次回臺指現貨（TXF-S）與各月份台指期（TXF??-F），美國機房連得到
LIVE_URL = "https://mis.taifex.com.tw/futures/api/getQuoteList"
LIVE_BODY = {"MarketType": "0", "SymbolType": "F", "KindID": "1", "CID": "TXF", "ExpireMonth": "",
             "RowSize": "全部", "PageNo": "", "SortColumn": "", "AscDesc": "A"}
INTRADAY_KEEP_DAYS = 14
TICK_URL = "https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/Daily_{:%Y_%m_%d}.zip"   # 期貨每筆成交（盤後，約 2～3MB）
MI5_URL = "https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_INDEX"                                         # 加權指數每 5 秒（盤後）
TZ = dt.timezone(dt.timedelta(hours=8))


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
        # 盤中大盤 vs 台指期近月（每 2 分鐘一筆，只留近兩週）
        self._q("""CREATE TABLE IF NOT EXISTS fut_intraday (
            d TEXT NOT NULL, t TEXT NOT NULL, spot REAL, fut REAL, spot_ref REAL, fut_ref REAL, contract TEXT,
            PRIMARY KEY (d, t))""")
        # 台指期近月每日收盤（一般時段），算正逆價差＝近月收盤 − 加權指數收盤
        self._q("""CREATE TABLE IF NOT EXISTS tx_close (d TEXT PRIMARY KEY, month TEXT, close REAL)""")
        self._live_at = 0.0

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
            if dt.datetime.now(TZ).strftime("%H:%M") >= "13:50":
                try:
                    if self.intraday_incomplete(today):
                        logger.info(f"[fut] 盤後補今日走勢：{self.backfill_intraday(today)}")
                except Exception as e:
                    logger.info(f"[fut] 盤後補今日走勢略過：{e}")
            try:
                n_bs = self.update_basis(today)
            except Exception as e:
                n_bs = f"失敗：{e}"
                logger.warning(f"[fut] 台指期近月收盤抓取失敗：{e}")
            self._cache.clear()
            return f"寫入 {total} 筆，最新 {self.last_date()}；小台未平倉 {n_oi} 天；加權指數 {n_tx} 天；台指期近月 {n_bs} 天"

    # ---------- 正逆價差（每日） ----------
    @staticmethod
    def fetch_tx_close(start, end):
        """台指期每日行情（期交所 futDataDown，一次約一個月）→ {日期: (近月月份, 收盤價)}；
        只看一般時段、6 碼月份（排除週契約），取當天有收盤價的最近月份"""
        r = requests.post(OI_URL, headers=UA, timeout=120, data={
            "down_type": "1", "commodity_id": "TX", "commodity_id2": "",
            "queryStartDate": start.strftime("%Y/%m/%d"), "queryEndDate": end.strftime("%Y/%m/%d")})
        r.raise_for_status()
        rows = list(csv.reader(io.StringIO(r.content.decode("cp950", errors="replace"))))
        if not rows or rows[0][:2] != ["交易日期", "契約"]:
            raise ValueError("期交所行情回應格式不符（查詢區間可能太長）")
        h = rows[0]
        im, ic, it = h.index("到期月份(週別)"), h.index("收盤價"), h.index("交易時段")
        out = {}
        for x in rows[1:]:
            if len(x) <= max(im, ic, it) or x[it].strip() != "一般":
                continue
            m, c = x[im].strip(), x[ic].strip().replace(",", "")
            if not (len(m) == 6 and m.isdigit()):
                continue
            try:
                c = float(c)
            except ValueError:
                continue
            d = x[0].strip().replace("/", "-")
            if d not in out or m < out[d][0]:
                out[d] = (m, c)
        return out

    def update_basis(self, today=None):
        today = today or dt.datetime.now(TZ).date()
        r = self._q("SELECT MAX(d) AS d FROM tx_close", fetch="one") or {}
        s = dt.date.fromisoformat(r["d"]) if r.get("d") else today - dt.timedelta(days=BACKFILL_DAYS)
        n = 0
        while s <= today:
            e = min(s + dt.timedelta(days=27), today)
            for d, (m, c) in self.fetch_tx_close(s, e).items():
                self._q("INSERT INTO tx_close (d, month, close) VALUES (?, ?, ?) "
                        "ON CONFLICT (d) DO UPDATE SET month = EXCLUDED.month, close = EXCLUDED.close", (d, m, c))
                n += 1
            s = e + dt.timedelta(days=1)
            time.sleep(1)
        return n

    def basis(self, days=120):
        rows = self._q("SELECT t.d, t.month, t.close AS fut, x.close AS spot FROM tx_close t JOIN taiex_close x ON x.d = t.d "
                       "ORDER BY t.d DESC LIMIT ?", (int(days),), fetch="all") or []
        rows = rows[::-1]
        return {"dates": [r["d"] for r in rows], "fut": [r["fut"] for r in rows], "spot": [r["spot"] for r in rows],
                "basis": [round(r["fut"] - r["spot"], 2) for r in rows], "month": rows[-1]["month"] if rows else None}

    # ---------- 盤中大盤 vs 台指期 ----------
    @staticmethod
    def fetch_live():
        r = requests.post(LIVE_URL, json=LIVE_BODY, timeout=15,
                          headers={**UA, "Referer": "https://mis.taifex.com.tw/futures/RegularSession/EquityIndices/FuturesDomestic/"})
        r.raise_for_status()
        q = (r.json().get("RtData") or {}).get("QuoteList") or []
        num = lambda v: float(str(v).replace(",", "")) if v not in (None, "", "-") else None
        spot = next((x for x in q if str(x.get("SymbolID", "")).endswith("-S")), None)
        fut = next((x for x in q if str(x.get("SymbolID", "")).endswith("-F") and num(x.get("CLastPrice"))), None)
        if not spot or not fut:
            raise ValueError(f"期交所盤中行情格式不符：{str(q)[:150]}")
        return {"d": spot.get("CDate") or fut.get("CDate"),
                "spot": num(spot.get("CLastPrice")), "spot_ref": num(spot.get("CRefPrice")), "spot_time": spot.get("CTime", ""),
                "fut": num(fut.get("CLastPrice")), "fut_ref": num(fut.get("CRefPrice")), "fut_time": fut.get("CTime", ""),
                "contract": fut.get("DispCName", "")}

    def poll_live(self, force=False):
        """存一筆盤中快照（平日 08:45～13:46）；force＝不管時間窗（測試用）"""
        now = dt.datetime.now(TZ)
        hm = now.strftime("%H:%M")
        if not force and (now.weekday() >= 5 or not ("08:45" <= hm <= "13:46")):
            return "非盤中"
        q = self.fetch_live()
        today = now.strftime("%Y%m%d")
        if q["d"] != today:
            return f"期交所資料日 {q['d']} 不是今天（可能休市）"
        spot = q["spot"] if q["spot_time"] >= "090000" else None      # 現貨 09:00 才開盤，之前是昨收
        self._q("INSERT INTO fut_intraday (d, t, spot, fut, spot_ref, fut_ref, contract) VALUES (?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT (d, t) DO UPDATE SET spot = EXCLUDED.spot, fut = EXCLUDED.fut, spot_ref = EXCLUDED.spot_ref, "
                "fut_ref = EXCLUDED.fut_ref, contract = EXCLUDED.contract",
                (now.date().isoformat(), hm, spot, q["fut"], q["spot_ref"], q["fut_ref"], q["contract"]))
        self._live_at = time.time()
        self._q("DELETE FROM fut_intraday WHERE d < ?", ((now.date() - dt.timedelta(days=INTRADAY_KEEP_DAYS)).isoformat(),))
        return f"{hm} 加權 {spot} 台指期 {q['fut']}"

    def backfill_intraday(self, day=None):
        """盤後補當天完整走勢（網站沒在跑或剛部署時）：加權指數用證交所每 5 秒指數（收盤後就有），
        台指期近月用期交所每筆成交檔（日盤約 16:45 才併進檔案，之前只補得到加權）；每 2 分鐘取一點，已有的值不覆蓋"""
        import zipfile
        day = day or dt.datetime.now(TZ).date()
        ds = day.strftime("%Y%m%d")
        ticks = {}                                               # 月份 → [(HHMMSS, 價)]
        try:
            r = requests.get(TICK_URL.format(day), headers=UA, timeout=120)
            if r.status_code == 200 and r.content[:2] == b"PK":
                z = zipfile.ZipFile(io.BytesIO(r.content))
                for line in z.read(z.namelist()[0]).decode("cp950", errors="replace").splitlines():
                    p = line.split(",")
                    if len(p) < 5 or p[0] != ds or p[1].strip() != "TX":
                        continue
                    m, t = p[2].strip(), p[3].strip()
                    if len(m) == 6 and m.isdigit() and "084500" <= t <= "134500":
                        ticks.setdefault(m, []).append((t, float(p[4])))
        except Exception as e:
            logger.info(f"[fut] 每筆成交檔讀取失敗：{e}")
        month = min(ticks) if ticks else None
        fut_ticks = sorted(ticks[month]) if month else []
        j = requests.get(MI5_URL, params={"response": "json", "date": ds}, headers=UA, timeout=30).json()
        spot_at = {}
        for row in j.get("data") or []:
            if row and row[0].endswith(":00"):
                try:
                    spot_at[row[0][:5]] = float(str(row[1]).replace(",", ""))
                except ValueError:
                    pass
        prev = lambda tbl: (self._q(f"SELECT close FROM {tbl} WHERE d < ? ORDER BY d DESC LIMIT 1", (day.isoformat(),), fetch="one") or {}).get("close")
        spot_ref, fut_ref = prev("taiex_close"), prev("tx_close")
        slots = []
        t = dt.datetime.combine(day, dt.time(8, 46))
        while t.time() <= dt.time(13, 45):
            slots.append(t.strftime("%H:%M"))
            t += dt.timedelta(minutes=2)
        if slots[-1] != "13:45":
            slots.append("13:45")
        if not fut_ticks and not spot_at:
            raise ValueError(f"{ds} 盤後資料還沒出來")
        contract = f"臺指期 {month[:4]}/{month[4:]}" if month else None
        n, i, last = 0, 0, None
        for hm in slots:
            lim = hm.replace(":", "") + "59"
            while i < len(fut_ticks) and fut_ticks[i][0] <= lim:
                last = fut_ticks[i][1]
                i += 1
            spot = spot_at.get(hm) if "09:00" <= hm <= "13:30" else None
            if last is None and spot is None:
                continue
            self._q("INSERT INTO fut_intraday (d, t, spot, fut, spot_ref, fut_ref, contract) VALUES (?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (d, t) DO UPDATE SET spot = COALESCE(fut_intraday.spot, EXCLUDED.spot), "
                    "fut = COALESCE(fut_intraday.fut, EXCLUDED.fut), spot_ref = COALESCE(fut_intraday.spot_ref, EXCLUDED.spot_ref), "
                    "fut_ref = COALESCE(fut_intraday.fut_ref, EXCLUDED.fut_ref), contract = COALESCE(fut_intraday.contract, EXCLUDED.contract)",
                    (day.isoformat(), hm, spot, last, spot_ref, fut_ref, contract))
            n += 1
        return f"{n} 點（加權 {len(spot_at)}、台指期{'成交 ' + str(len(fut_ticks)) + ' 筆' if fut_ticks else '每筆成交檔還沒出來'}）"

    def intraday_incomplete(self, day):
        r = self._q("SELECT COUNT(spot) AS s, COUNT(fut) AS f FROM fut_intraday WHERE d = ?", (day.isoformat(),), fetch="one") or {}
        return (r.get("s") or 0) < 100 or (r.get("f") or 0) < 120

    def intraday(self):
        """最近一個有盤中資料的交易日；盤中開頁時若超過 60 秒沒更新就先抓一筆；
        收盤後當天資料不完整（網站剛部署、排程沒跑）就用盤後檔補齊（10 分鐘內最多一次）"""
        now = dt.datetime.now(TZ)
        if (now.weekday() < 5 and "08:45" <= now.strftime("%H:%M") <= "13:46"
                and time.time() - self._live_at > 60):
            try:
                self.poll_live()
            except Exception as e:
                logger.warning(f"[fut] 盤中行情抓取失敗：{e}")
        if now.weekday() < 5 and now.strftime("%H:%M") >= "13:50" and time.time() - getattr(self, "_bf_at", 0) > 600:
            if self.intraday_incomplete(now.date()):
                self._bf_at = time.time()
                try:
                    import scheduler      # 要下載每筆成交 zip，另開子程序做，記憶體跑完就還
                    job = scheduler.subprocess_job("fut_intraday", lambda: self.backfill_intraday(now.date()), timeout=300)
                    logger.info(f"[fut] 盤後補今日走勢：{job()}")
                except Exception as e:
                    logger.warning(f"[fut] 盤後補今日走勢失敗：{e}")
        r = self._q("SELECT MAX(d) AS d FROM fut_intraday", fetch="one") or {}
        if not r.get("d"):
            return {"date": None, "rows": []}
        rows = self._q("SELECT t, spot, fut, spot_ref, fut_ref, contract FROM fut_intraday WHERE d = ? ORDER BY t",
                       (r["d"],), fetch="all") or []
        last = rows[-1]
        return {"date": r["d"], "rows": [[x["t"], x["spot"], x["fut"]] for x in rows],
                "spot_ref": last["spot_ref"], "fut_ref": last["fut_ref"], "contract": last["contract"],
                "live": r["d"] == now.date().isoformat() and now.strftime("%H:%M") <= "13:46"}

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
    import scheduler
    scheduler.spawn("fut_startup", lambda: startup(store), after=lambda: store._cache.clear())
    return store


def startup(st):
    """啟動時補資料（首次補兩年、往前補、交易口數、加權收盤、近月收盤）；預設在獨立子程序跑（jobs.py fut_startup）"""
    no_oi = not (st._q("SELECT COUNT(*) AS n FROM fut_oi", fetch="one") or {}).get("n")
    out = []
    if not st.last_date() or no_oi:
        out.append("首次補資料：" + str(st.update()))
    n = st.backfill_older()
    if n:
        out.append(f"往前補到兩年 {n} 筆")
    n = st.backfill_trade()
    if n:
        out.append(f"補交易口數與金額 {n} 筆")
    if not (st._q("SELECT COUNT(*) AS n FROM taiex_close", fetch="one") or {}).get("n"):
        out.append(f"補加權指數收盤 {st.update_taiex()} 天")
    if not (st._q("SELECT COUNT(*) AS n FROM tx_close", fetch="one") or {}).get("n"):
        out.append(f"補台指期近月收盤 {st.update_basis()} 天")
    return "；".join(out) or "不用補"


_last_try = 0.0


def refresh_if_stale():
    """有人打開期貨頁時：平日 15:00 後、資料庫還沒有今天的資料，就在背景抓一次（10 分鐘內最多一次）。
    期交所三大法人期貨資料一天只在盤後公布一次（約 15:00），盤中沒有可抓的新資料。"""
    global _last_try
    if not store or store.lock.locked():
        return
    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8)))
    if now.weekday() >= 5 or now.hour < 15 or time.time() - _last_try < 600:
        return
    if (store.last_date() or "") >= now.date().isoformat():
        return
    _last_try = time.time()

    def run():
        try:
            import scheduler
            job = scheduler.subprocess_job("fut", store.update, after=lambda: store._cache.clear())
            logger.info("[fut] 開頁觸發更新：" + str(job()))
        except Exception as e:
            logger.warning(f"[fut] 開頁觸發更新失敗：{e}")
    threading.Thread(target=run, daemon=True).start()


def create_router(templates):
    router = APIRouter()

    @router.get("/futures", response_class=HTMLResponse)
    async def futures_page(request: Request):
        await run_in_threadpool(refresh_if_stale)
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

    @router.get("/api/futures/intraday")
    async def futures_intraday():
        """盤中加權指數 vs 台指期近月（每 2 分鐘一筆）"""
        if not store:
            raise HTTPException(status_code=503, detail="資料庫無法使用")
        return JSONResponse(await run_in_threadpool(store.intraday))

    @router.get("/api/futures/basis")
    async def futures_basis(days: int = Query(120, ge=20, le=750)):
        """每日正逆價差＝台指期近月收盤 − 加權指數收盤（點）"""
        if not store:
            raise HTTPException(status_code=503, detail="資料庫無法使用")
        return JSONResponse(await run_in_threadpool(store.basis, days))

    return router
