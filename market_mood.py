# -*- coding: utf-8 -*-
"""市場情緒指標：資料庫、燈號判斷、賣出條件、自動結論、頁面路由

資料表 mood_obs(ind, d, v, extra)：每個指標每個日期一筆
排程工作 mood（scheduler.py）：預設週二～六 07:30（台北），美股收盤後
頁面 /market-mood（公開）、API /api/market-mood
門檻固定，沿用使用者「總體風險掃描」提示詞 v3；拿掉抓不到的 NAAIM、高收益債利差、內部人買賣比、AAII 持股比重
"""
import datetime as dt
import json
import logging
import threading
import time

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.concurrency import run_in_threadpool

import mood_fetch as F

logger = logging.getLogger(__name__)
TERMS = [("short", "短期", "天－週", "判斷短線是否過熱、會不會回檔"),
         ("mid", "中期", "週－月", "判斷槓桿、資金與廣度是否出現趨勢轉折"),
         ("long", "長期", "月－年", "判斷評價與景氣是否來到結構性循環頭部")]


class MoodStore:
    def __init__(self, db):
        self.db = db
        self.lock = threading.Lock()
        self._cache = None
        self._cache_at = 0
        self.last_errors = {}
        self._run("""CREATE TABLE IF NOT EXISTS mood_obs (
            ind TEXT NOT NULL, d TEXT NOT NULL, v DOUBLE PRECISION, extra TEXT,
            PRIMARY KEY (ind, d))""")

    @property
    def pg(self):
        return self.db.db_type == "postgresql"

    def _run(self, sql, params=(), fetch=False, many=None):
        if not self.pg:
            sql = sql.replace("DOUBLE PRECISION", "REAL")
        with self.db.get_connection() as conn:
            cur = conn.cursor()
            if many is not None:
                if self.pg:
                    from psycopg2.extras import execute_values
                    execute_values(cur, sql.replace("VALUES (%s, %s, %s, %s)", "VALUES %s"), many, page_size=1000)
                else:
                    cur.executemany(sql.replace("%s", "?"), many)
            else:
                cur.execute(sql if self.pg else sql.replace("%s", "?"), params)
            rows = cur.fetchall() if fetch else None
            if not self.pg:
                conn.commit()
        if rows is None:
            return None
        return [tuple(r.values()) if isinstance(r, dict) else tuple(r) for r in rows]

    def save(self, ind, rows):
        if not rows:
            return 0
        self._run("INSERT INTO mood_obs (ind, d, v, extra) VALUES (%s, %s, %s, %s) "
                  "ON CONFLICT (ind, d) DO UPDATE SET v = excluded.v, extra = excluded.extra",
                  many=[(ind, d, v, json.dumps(e, ensure_ascii=False) if e else None) for d, v, e in rows])
        self._cache = None
        return len(rows)

    def dates(self, ind):
        return {r[0] for r in self._run("SELECT d FROM mood_obs WHERE ind = %s", (ind,), fetch=True)}

    def series(self):
        out = {}
        for ind, d, v, e in self._run("SELECT ind, d, v, extra FROM mood_obs ORDER BY ind, d", fetch=True):
            out.setdefault(ind, []).append((d, v, json.loads(e) if e else {}))
        return out

    def count(self):
        return self._run("SELECT COUNT(*) FROM mood_obs", fetch=True)[0][0]

    # ---------- 抓取 ----------
    def update(self):
        """抓全部指標；個別失敗不影響其他。回傳摘要字串，全部失敗才丟例外"""
        with self.lock:
            ok, fail = [], []
            for ind, fn in F.FETCHERS.items():
                try:
                    rows = fn(have=self.dates(ind)) if ind in F.NEEDS_HAVE else fn()
                    self.save(ind, rows)
                    ok.append(ind)
                    self.last_errors.pop(ind, None)
                except Exception as e:
                    fail.append(ind)
                    self.last_errors[ind] = f"{type(e).__name__}: {str(e)[:200]}"
                    logger.error(f"[mood] {ind} 抓取失敗：{e}")
            self._cache = None
            if not ok:
                raise RuntimeError("全部抓取失敗：" + "；".join(f"{k} {v}" for k, v in self.last_errors.items()))
            return f"成功 {len(ok)}/{len(F.FETCHERS)}" + (f"；失敗：{'、'.join(fail)}" if fail else "")

    # ---------- 給頁面的完整結果（快取 10 分鐘）----------
    def report(self):
        if self._cache and time.time() - self._cache_at < 600:
            return self._cache
        self._cache = build_report(self.series(), self.last_errors)
        self._cache_at = time.time()
        return self._cache


# ───────────────────────── 計算 ─────────────────────────
def _last(rows, n=1):
    return rows[-n] if len(rows) >= n else None


def _asof(rows, day):
    """rows 中日期 <= day 的最後一筆"""
    cand = [r for r in rows if r[0] <= day]
    return cand[-1] if cand else None


def _days_old(day):
    return (dt.date.today() - dt.date.fromisoformat(day)).days


def _fmt(v, nd=2):
    return "—" if v is None else f"{v:,.{nd}f}"


def _arrow(cur, prev):
    if cur is None or prev is None:
        return "→", None
    d = cur - prev
    return ("↑" if d > 0 else "↓" if d < 0 else "→"), d


def derive(S):
    """由原始序列算出衍生指標：保證金佔 GDP、巴菲特指標、騰落線"""
    gdp = S.get("gdp", [])
    D = {}
    if S.get("margin") and gdp:
        D["margin_gdp"] = [(d, round(v / 1e6 / g[1] * 100, 2), {}) for d, v, _ in S["margin"]
                           if (g := _asof(gdp, d))]
    if S.get("w5000") and gdp:
        D["buffett"] = [(d, round(v / 1000 / g[1] * 100, 1), {}) for d, v, _ in S["w5000"]
                        if (g := _asof(gdp, d)) and d >= "2017-01-01"]
    if S.get("nyse_ad"):
        cum, line = 0, []
        for d, v, e in S["nyse_ad"]:
            cum += v
            line.append((d, cum, e))
        D["ad_line"] = line
    return D


def _ad_divergence(spx, ad):
    """騰落線頂部背離：S&P 500 近 5 個交易日創 60 日新高，騰落線卻沒有同步創 60 日新高"""
    if len(ad) < 20:
        return None, f"騰落線資料累積中（{len(ad)}/20 個交易日）"
    ad_days = [r[0] for r in ad]
    sp = [r for r in spx if r[0] >= ad_days[0]]
    if len(sp) < 20:
        return None, "S&P 500 資料不足"
    window = 60
    recent_sp = sp[-5:]
    sp_hi = max(v for _, v, _ in sp[-window:])
    sp_new_high = any(v >= sp_hi for _, v, _ in recent_sp)
    ad_hi = max(v for _, v, _ in ad[-window:])
    ad_new_high = any(v >= ad_hi for _, v, _ in ad[-5:])
    if sp_new_high and not ad_new_high:
        return True, "S&P 500 近 5 日創新高，騰落線沒有跟上"
    if sp_new_high:
        return False, "S&P 500 創新高，騰落線同步創高，沒有背離"
    return False, f"S&P 500 距近 {min(window, len(sp))} 日高點 {(sp[-1][1] / sp_hi - 1) * 100:.1f}%，背離前提不成立"


IND = [
    # id, term, 名稱, 單位格式, 門檻說明, 頻率, 圖表視窗（天）
    ("vix", "short", "VIX 恐慌指數", "", "<13 自滿／>25 恐慌", "日", 365),
    ("cnn_fg", "short", "CNN 恐懼貪婪指數", "", ">75 極度貪婪", "日", 365),
    ("tw_vix", "short", "台股 VIX（臺指選擇權波動率指數）", "", "<15 自滿／>30 恐慌", "日", 365),
    ("put_call", "short", "CBOE 個股賣權買權比", "", "<0.55 自滿", "日", 120),
    ("aaii", "short", "AAII 散戶情緒（看多比）", "%", "看多 >45% 或多空差 >+20", "週", 400),
    ("margin", "mid", "FINRA 保證金負債", "", "年增 >+30%", "月", 400),
    ("margin_gdp", "mid", "保證金負債佔 GDP", "%", ">4.0%", "月", 400),
    ("ipo", "mid", "美股 IPO 募資額（今年累計）", "", "募資額年增 >100% 留意", "日", 365),
    ("ad_line", "mid", "紐約證交所騰落線", "", "指數創新高、騰落線未同步", "日", 365),
    ("bofa", "mid", "美銀牛熊指標", "", ">8.0 反向賣出訊號", "週", 365),
    ("buffett", "long", "巴菲特指標（總市值／GDP）", "%", ">200% 極度高估", "月", 3650),
    ("cape", "long", "席勒本益比 CAPE", "", ">40 極端（歷史最高 44.19）", "月", 3650),
    ("t10y2y", "long", "美債 10 年減 2 年利差", "", "<0 殖利率倒掛", "日", 640),
    ("lei", "long", "美國經濟諮商理事會領先指標", "", "近 6 個月 < -4%", "月", 3650),
]
STALE = {"日": 6, "週": 12, "月": 75}
SHORT = {"vix": "VIX 恐慌指數", "tw_vix": "台股 VIX", "cnn_fg": "CNN 恐懼貪婪", "aaii": "AAII 散戶看多", "put_call": "個股賣權買權比",
         "margin": "FINRA 保證金負債", "margin_gdp": "保證金佔 GDP", "ipo": "IPO 募資額", "ad_line": "NYSE 騰落線",
         "bofa": "美銀牛熊指標", "buffett": "巴菲特指標", "cape": "席勒本益比 CAPE", "t10y2y": "美債 10Y−2Y 利差",
         "lei": "領先指標 LEI"}
CHART_ZONES = {
    "cnn_fg": [(0, 25, "fear2", "極度恐懼"), (25, 45, "fear", "恐懼"), (45, 55, "neutral", "中性"),
               (55, 75, "amber", "貪婪"), (75, 100, "red", "極度貪婪")],
}
YFIXED = {"cnn_fg": (0, 100)}
YCLAMP = { "aaii": (0, 70), "bofa": (0, 10)}
SOURCES = {
    "vix": ("CBOE", "https://www.cboe.com/tradable_products/vix/"),
    "tw_vix": ("臺灣期貨交易所", "https://www.taifex.com.tw/cht/7/vixDaily3MNew"),
    "cnn_fg": ("CNN", "https://www.cnn.com/markets/fear-and-greed"),
    "aaii": ("AAII", "https://www.aaii.com/sentimentsurvey"),
    "put_call": ("CBOE", "https://www.cboe.com/us/options/market_statistics/daily/"),
    "margin": ("FINRA", "https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics"),
    "margin_gdp": ("FINRA ÷ GDP（multpl）", "https://www.multpl.com/us-gdp"),
    "ipo": ("Renaissance Capital", "https://www.renaissancecapital.com/IPO-Center/Stats"),
    "ad_line": ("WSJ Markets Diary（本站逐日累計）", "https://www.wsj.com/market-data/stocks/marketsdiary"),
    "bofa": ("BofA Flow Show（經 Finvaulta）", "https://finvaulta.com/research/bank-of-america/series/the-flow-show"),
    "buffett": ("Wilshire 5000 ÷ GDP", "https://www.multpl.com/us-gdp"),
    "cape": ("multpl", "https://www.multpl.com/shiller-pe"),
    "t10y2y": ("美國財政部", "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve"),
    "lei": ("The Conference Board", "https://www.conference-board.org/topics/us-leading-indicators"),
}


def _judge(i, rows, S, D):
    """回傳 dict：signal(green/yellow/red/gray)、label、display、note、lines（圖上的門檻線）"""
    d, v, e = rows[-1]
    prev = rows[-2] if len(rows) > 1 else None
    r = {"signal": "green", "label": "正常", "lines": []}
    if i == "vix":
        r["display"] = _fmt(v)
        r["lines"] = [(25, "恐慌 25"), (13, "自滿 13")]
        if v > 25: r.update(signal="red", label="恐慌")
        elif v < 13: r.update(signal="yellow", label="自滿")
        r["note"] = (f"目前 {v:.2f}，" + ("站上 25 的恐慌線，市場正在付高價買保險。" if v > 25 else
                     "低於 13 的自滿線，避險很便宜，市場很放心。" if v < 13 else
                     f"介於 13 到 25 之間，距自滿線 {v - 13:.1f} 點、距恐慌線 {25 - v:.1f} 點。"))
    elif i == "tw_vix":
        r["display"] = _fmt(v)
        r["lines"] = [(30, "恐慌 30"), (15, "自滿 15")]
        if v > 30: r.update(signal="red", label="恐慌")
        elif v < 15: r.update(signal="yellow", label="自滿")
        us = (S.get("vix") or [[None, None]])[-1][1]
        mx = max(x[1] for x in rows[-63:])
        r["note"] = (f"目前 {v:.2f}，近 3 個月最高 {mx:.2f}。" +
                     ("高於 30，台股選擇權避險需求很高。" if v > 30 else "低於 15，台股市場很放心。" if v < 15 else
                      "介於 15 到 30 之間。") +
                     (f"同日美股 VIX {us:.2f}，台股波動預期{'高於' if v > us else '低於'}美股。" if us else ""))
    elif i == "cnn_fg":
        rating = {"extreme fear": "極度恐懼", "fear": "恐懼", "neutral": "中性", "greed": "貪婪",
                  "extreme greed": "極度貪婪"}.get((e or {}).get("rating", ""), "")
        r["display"] = f"{v:.0f}"
        r["sub"] = rating
        r["lines"] = []
        if v >= 75: r.update(signal="red", label="極度貪婪")
        elif v >= 55: r.update(signal="yellow", label="貪婪")
        mx = max(x[1] for x in rows[-63:])
        r["note"] = (f"目前 {v:.0f}，近 3 個月最高 {mx:.0f}。" +
                     ("已進入極度貪婪區，短線過熱。" if v >= 75 else
                      "落在恐懼區，短線情緒偏悲觀，過熱風險低。" if v < 45 else "情緒中性偏多，沒有過熱。"))
    elif i == "aaii":
        sp, bear = (e or {}).get("spread"), (e or {}).get("bearish")
        r["display"] = f"{v:.1f}%"
        r["sub"] = f"看空 {bear:.1f}%／多空差 {sp:+.1f}" if sp is not None else ""
        r["lines"] = [(45, "看多 45%")]
        if v > 45 or (sp is not None and sp > 20): r.update(signal="red", label="散戶過度樂觀")
        r["note"] = (f"看多 {v:.1f}%、看空 {bear:.1f}%，多空差 {sp:+.1f}（長期平均 +6.5）。" +
                     ("看空超過 40.7%，散戶明顯悲觀，歷史上常是反向買點。" if bear and bear > 40.7 else
                      "散戶一面倒看多，要小心。" if r["signal"] == "red" else "散戶情緒沒有過熱。"))
        r["extra_series"] = [{"name": "看空 %", "data": [(x[0], (x[2] or {}).get("bearish")) for x in rows]}]
        r["chart2"] = {"title": "多空差（看多 − 看空）", "label": "多空差",
                       "rows": [(x[0], (x[2] or {}).get("spread")) for x in rows if (x[2] or {}).get("spread") is not None],
                       "lines": [(20, "極端樂觀 +20"), (6.5, "長期平均 +6.5"), (-20, "極端悲觀 −20")],
                       "zones": [(20, 60, "red", "極端樂觀"), (-60, -20, "fear", "極端悲觀")], "type": "bar"}
    elif i == "put_call":
        r["display"] = f"{v:.2f}"
        r["lines"] = [(0.55, "自滿 0.55")]
        if v < 0.55: r.update(signal="red", label="自滿")
        avg = sum(x[1] for x in rows[-20:]) / min(20, len(rows))
        r["note"] = f"目前 {v:.2f}，近 20 日平均 {avg:.2f}。" + (
            "跌破 0.55，買保險的人很少，是典型的自滿訊號。" if v < 0.55 else "沒有跌破 0.55 的自滿線。")
    elif i == "margin":
        yr = _asof(rows, (dt.date.fromisoformat(d) - dt.timedelta(days=360)).isoformat())
        yoy = (v / yr[1] - 1) * 100 if yr and yr[0] < d else None
        mom = (v / prev[1] - 1) * 100 if prev else None
        r["display"] = f"{v / 1e6:.2f} 兆美元"
        r["sub"] = (f"年增 {yoy:+.1f}%" if yoy is not None else "") + (f"／月增 {mom:+.1f}%" if mom is not None else "")
        if yoy is not None and yoy > 30: r.update(signal="red", label="槓桿過高")
        r["note"] = (f"{d[:7]} 融資餘額 {v / 1e6:.2f} 兆美元" + (f"，年增 {yoy:+.1f}%" if yoy is not None else "") +
                     (f"、月增 {mom:+.1f}%" if mom is not None else "") + "。" +
                     ("年增超過 30%，槓桿擴張過快。" if r["signal"] == "red" else "年增沒有超過 30%。"))
        r["chart_scale"] = 1e-6
    elif i == "margin_gdp":
        r["display"] = f"{v:.2f}%"
        r["lines"] = [(4.0, "警戒 4.0%")]
        if v > 4.0: r.update(signal="red", label="偏高")
        r["note"] = f"融資餘額佔 GDP {v:.2f}%（長期平均約 3.06%）。" + ("高於 4.0% 的警戒線。" if v > 4.0 else "低於 4.0% 的警戒線。")
    elif i == "ipo":
        ex = e or {}
        r["display"] = f"{v:,.1f} 十億美元"
        r["sub"] = f"掛牌 {ex.get('priced_count', '—')} 件（年增 {ex.get('priced_yoy', 0):+.1f}%）"
        if ex.get("proceeds_yoy", 0) > 100: r.update(signal="yellow", label="募資額暴增")
        r["note"] = (f"今年累計募資 {v:,.1f} 十億美元、年增 {ex.get('proceeds_yoy', 0):+.1f}%；"
                     f"掛牌件數 {ex.get('priced_count', '—')} 件、年增 {ex.get('priced_yoy', 0):+.1f}%。" +
                     ("金額暴增但件數沒有同步增加，是少數超大案帶動，不是全民搶掛牌。"
                      if ex.get("proceeds_yoy", 0) > 100 and ex.get("priced_yoy", 0) < 20 else ""))
    elif i == "ad_line":
        div, msg = _ad_divergence(S.get("spx", []), rows)
        ex = (S.get("nyse_ad") or [[None, None, {}]])[-1][2] or {}
        r["display"] = f"{v:+,.0f}"
        r["sub"] = f"當日上漲 {ex.get('advancing', '—')}／下跌 {ex.get('declining', '—')} 家"
        if div is None: r.update(signal="gray", label="累積中")
        elif div: r.update(signal="red", label="頂部背離")
        r["note"] = msg + "。騰落線＝每天上漲家數減下跌家數的累計，本站從開始抓取的那天起累計。"
        r["divergence"] = div
    elif i == "bofa":
        r["display"] = f"{v:.1f}"
        r["lines"] = [(8, "賣出訊號 8.0"), (2, "買進訊號 2.0")]
        if v > 8: r.update(signal="red", label="賣出訊號")
        r["note"] = f"{d} 的 Flow Show 週報讀數 {v:.1f}。" + ("高於 8.0，代表資金已經非常樂觀，是美銀的反向賣出訊號。" if v > 8 else
                                                         "沒有高於 8.0。")
    elif i == "buffett":
        r["display"] = f"{v:.1f}%"
        r["lines"] = [(200, "極度高估 200%")]
        if v > 200: r.update(signal="red", label="極度高估")
        elif v > 150: r.update(signal="yellow", label="高估")
        r["note"] = f"美股總市值約為 GDP 的 {v:.0f}%（長期平均約 166%）。" + ("高於 200%，評價在歷史極端區。" if v > 200 else "")
    elif i == "cape":
        r["display"] = f"{v:.2f}"
        r["lines"] = [(40, "極端 40"), (44.19, "1999 高點 44.19")]
        if v > 40: r.update(signal="red", label="極端")
        elif v > 30: r.update(signal="yellow", label="偏高")
        r["note"] = f"目前 {v:.2f}，歷史平均約 17，1999 年 12 月最高 44.19（距離 {(44.19 / v - 1) * 100:.1f}%）。"
    elif i == "t10y2y":
        ex = e or {}
        r["display"] = f"{v:+.2f}%"
        r["sub"] = f"10 年 {ex.get('y10', '—')}%／2 年 {ex.get('y2', '—')}%"
        r["lines"] = [(0, "倒掛 0")]
        if v < 0: r.update(signal="red", label="倒掛")
        r["note"] = f"10 年期減 2 年期 {v:+.2f} 個百分點。" + ("殖利率曲線倒掛，歷史上常領先衰退。" if v < 0 else "曲線正斜率，沒有倒掛。")
    elif i == "lei":
        six = (e or {}).get("six_month")
        r["display"] = f"{v:.1f}"
        r["sub"] = (f"月變化 {(e or {}).get('mom', 0):+.1f}%" + (f"／近 6 個月 {six:+.1f}%" if six is not None else ""))
        if six is not None and six < -4: r.update(signal="red", label="衰退警戒")
        r["note"] = (f"{d[:7]} 指數 {v:.1f}" + (f"，近 6 個月 {six:+.1f}%" if six is not None else "") + "。" +
                     ("跌幅超過 4%，是衰退警戒。" if r["signal"] == "red" else "離 -4% 的衰退警戒線很遠，經濟是減速不是衰退。"))
    r["arrow"], r["change"] = _arrow(v, prev[1] if prev else None)
    return r


# 溫度計刻度：lo、hi、警戒區 [(起, 迄, 顏色, 說明)]；track 取的值見 _track_value
TRACK = {
    "vix":        (10, 40, [(10, 13, "amber", "自滿"), (25, 40, "red", "恐慌")], ""),
    "tw_vix":     (10, 50, [(10, 15, "amber", "自滿"), (30, 50, "red", "恐慌")], ""),
    "cnn_fg":     (0, 100, [(55, 75, "amber", "貪婪"), (75, 100, "red", "極度貪婪")], ""),
    "aaii":       (15, 65, [(45, 65, "red", "過度樂觀")], "看多 %"),
    "put_call":   (0.4, 1.0, [(0.4, 0.55, "red", "自滿")], ""),
    "margin":     (-20, 60, [(30, 60, "red", "年增過快")], "年增 %"),
    "margin_gdp": (2.0, 5.0, [(4.0, 5.0, "red", "偏高")], "%"),
    "ipo":        (-50, 500, [(100, 500, "amber", "募資暴增")], "募資年增 %"),
    "bofa":       (0, 10, [(8, 10, "red", "賣出訊號")], ""),
    "buffett":    (50, 250, [(150, 200, "amber", "高估"), (200, 250, "red", "極度高估")], "%"),
    "cape":       (10, 50, [(30, 40, "amber", "偏高"), (40, 50, "red", "極端")], ""),
    "t10y2y":     (-1.5, 2.0, [(-1.5, 0, "red", "倒掛")], "百分點"),
    "lei":        (-8, 4, [(-8, -4, "red", "衰退警戒")], "近 6 個月 %"),
}


def _track_value(i, rows):
    """溫度計上的位置：多數指標用原值；保證金用年增率、IPO 用募資年增、LEI 用近 6 個月變化"""
    def at(k):
        if len(rows) < k:
            return None
        d, v, e = rows[-k]
        if i == "margin":
            yr = _asof(rows[:len(rows) - k + 1], (dt.date.fromisoformat(d) - dt.timedelta(days=360)).isoformat())
            return round((v / yr[1] - 1) * 100, 1) if yr and yr[0] < d else None
        if i == "ipo":
            return (e or {}).get("proceeds_yoy")
        if i == "lei":
            return (e or {}).get("six_month")
        return v
    return at(1), at(2)


def build_report(S, errors=None):
    D = derive(S)
    allS = {**S, **D}
    items, terms = [], {t[0]: {"key": t[0], "name": t[1], "span": t[2], "desc": t[3], "items": []} for t in TERMS}
    for i, term, name, unit, thr, freq, window in IND:
        rows = allS.get(i) or []
        it = {"id": i, "term": term, "name": name, "short": SHORT[i], "yclamp": YCLAMP.get(i), "yfixed": YFIXED.get(i), "threshold": thr, "freq": freq,
              "source": SOURCES[i][0], "source_url": SOURCES[i][1]}
        if not rows:
            it.update(signal="gray", label="尚無資料", display="—", note=(errors or {}).get(i, "等待第一次抓取。"), chart=[])
        else:
            it.update(_judge(i, rows, S, D))
            last_d = rows[-1][0]
            it["date"] = last_d
            it["prev"] = rows[-2][1] if len(rows) > 1 else None
            it["stale"] = _days_old(last_d) > STALE[freq]
            since = (dt.date.today() - dt.timedelta(days=window)).isoformat()
            scale = it.pop("chart_scale", 1)
            it["chart"] = [(d, round(v * scale, 4)) for d, v, _ in rows if d >= since]
            if it.get("chart2"):
                it["chart2"]["data"] = [(d, v) for d, v in it["chart2"].pop("rows") if d >= since]
            if i in ("ad_line",) and len(it["chart"]) < 2:
                it["chart"] = [(d, v) for d, v, _ in rows]
            if i in TRACK:
                lo, hi, zones, unit = TRACK[i]
                cur, prv = _track_value(i, rows)
                pos = lambda x: None if x is None else round(max(0, min(1, (x - lo) / (hi - lo))) * 100, 1)
                it["track"] = {"lo": lo, "hi": hi, "unit": unit, "cur": cur, "prev": prv, "cur_pos": pos(cur),
                               "prev_pos": pos(prv), "zones": [{"from": pos(a), "to": pos(b), "tone": c, "label": t}
                                                               for a, b, c, t in zones]}
                it["zones_raw"] = CHART_ZONES.get(i) or (zones if i not in ("margin", "ipo", "lei") else [])
        items.append(it)
        terms[term]["items"].append(it)

    # 賣出條件
    def g(i):
        return next(x for x in items if x["id"] == i)
    vix_v = (S.get("vix") or [[None, None]])[-1][1]
    mg = S.get("margin") or []
    streak = 0
    for a, b in zip(reversed(mg[:-1]), reversed(mg)):
        if b[1] < a[1]: streak += 1
        else: break
    fg = S.get("cnn_fg") or []
    fg_hi = max((x[1] for x in fg[-63:]), default=None)
    fg_now = fg[-1][1] if fg else None
    bofa_v = (S.get("bofa") or [[None, None]])[-1][1]
    div = g("ad_line").get("divergence")
    sells = [
        {"name": "VIX 站上並守住 25", "rule": "高於 25", "now": _fmt(vix_v), "met": bool(vix_v and vix_v > 25), "partial": False},
        {"name": "保證金負債連續下降", "rule": "連續 3 個月下降", "now": f"已連續下降 {streak} 個月",
         "met": streak >= 3, "partial": 0 < streak < 3},
        {"name": "恐懼貪婪指數由高檔翻落", "rule": "由 75 以上跌回 50 以下",
         "now": f"目前 {_fmt(fg_now, 0)}，近 3 個月最高 {_fmt(fg_hi, 0)}",
         "met": bool(fg_hi and fg_hi >= 75 and fg_now < 50), "partial": bool(fg_hi and fg_hi >= 75 and fg_now >= 50)},
        {"name": "騰落線出現頂部背離", "rule": "指數創新高、騰落線未同步", "now": g("ad_line").get("note", "").split("。")[0],
         "met": bool(div), "partial": False},
        {"name": "美銀牛熊指標", "rule": "高於 8.0", "now": _fmt(bofa_v, 1), "met": bool(bofa_v and bofa_v > 8), "partial": False},
    ]
    n_met = sum(s["met"] for s in sells)
    if n_met >= 3: level = ("red", "開始分批出脫", "賣出條件同時成立 3 項以上。")
    elif n_met >= 2: level = ("yellow", "警戒升級", "賣出條件成立 2 項。")
    else: level = ("green", "未達出脫門檻", "需同時成立 3 項以上才開始分批出脫。")

    # 各期統計與結論
    for t in terms.values():
        valid = [x for x in t["items"] if x["signal"] != "gray"]
        t["n"] = len(t["items"])
        t["n_valid"] = len(valid)
        t["red"] = [x["name"] for x in valid if x["signal"] == "red"]
        t["yellow"] = [x["name"] for x in valid if x["signal"] == "yellow"]
        t["green"] = len(valid) - len(t["red"]) - len(t["yellow"])
        t["summary"] = _term_summary(t)

    spx = S.get("spx") or []
    spx_info = None
    if len(spx) >= 2:
        spx_info = {"date": spx[-1][0], "close": spx[-1][1], "chg": (spx[-1][1] / spx[-2][1] - 1) * 100,
                    "high": max(v for _, v, _ in spx[-252:]), }
        spx_info["off_high"] = (spx_info["close"] / spx_info["high"] - 1) * 100
    latest = max((x.get("date") or "" for x in items), default="")
    return {"items": items, "terms": list(terms.values()), "sells": sells, "n_met": n_met,
            "level": {"signal": level[0], "label": level[1], "desc": level[2]},
            "headline": _headline(terms, n_met, sells), "spx": spx_info, "latest": latest,
            "generated": dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M"),
            "errors": errors or {}}


def _term_summary(t):
    name = t["name"]
    if not t["n_valid"]:
        return f"{name}指標尚無資料。"
    red, yel = t["red"], t["yellow"]
    s = f"{name} {t['n_valid']} 項中，{len(red)} 項亮紅燈"
    s += f"（{'、'.join(red)}）" if red else ""
    s += f"、{len(yel)} 項黃燈（{'、'.join(yel)}）" if yel else ""
    s += "。"
    ratio = len(red) / t["n_valid"]
    if t["key"] == "short":
        s += ("短線沒有過熱跡象。" if not red else "短線局部過熱，留意回檔。" if ratio < 0.5 else "短線明顯過熱，避免追高。")
    elif t["key"] == "mid":
        s += ("資金面與廣度沒有轉折訊號。" if not red else "中期有轉折跡象，持續追蹤賣出條件。" if ratio < 0.5 else
              "中期多項轉弱，趨勢轉折風險升高。")
    else:
        s += ("評價與景氣都在合理區。" if not red else "評價偏高，長期期望報酬偏低，但不代表短期會跌。" if ratio < 0.75 else
              "評價全面在極端區，長期期望報酬偏低。")
    return s


def _headline(terms, n_met, sells):
    met = [s["name"] for s in sells if s["met"]]
    parts = [f"5 項賣出條件中成立 {n_met} 項" + (f"（{'、'.join(met)}）" if met else "") + "。"]
    for k in ("short", "mid", "long"):
        t = terms[k]
        parts.append(f"{t['name']} {len(t['red'])}/{t['n_valid']} 亮紅燈")
    return parts[0] + "、".join(parts[1:]) + "。"


# ───────────────────────── 初始化與路由 ─────────────────────────
store = None


def init(db_config):
    """建表；資料庫是空的就在背景先抓一次，讓剛部署的頁面有資料"""
    global store
    if not db_config:
        return None
    try:
        store = MoodStore(db_config)
    except Exception as e:
        logger.error(f"❌ 市場情緒資料表初始化失敗: {e}")
        return None
    if store.count() == 0:
        def _first():
            try:
                logger.info("[mood] 首次抓取：" + store.update())
            except Exception as e:
                logger.error(f"[mood] 首次抓取失敗：{e}")
        threading.Thread(target=_first, daemon=True).start()
    return store


def create_router(templates):
    router = APIRouter()

    @router.get("/market-mood", response_class=HTMLResponse)
    async def mood_page(request: Request):
        rep = await run_in_threadpool(store.report) if store else None
        return templates.TemplateResponse("market_mood.html", {"request": request, "rep": rep})

    @router.get("/api/market-mood")
    async def mood_api():
        if not store:
            return JSONResponse({"error": "資料庫無法使用"}, status_code=503)
        return await run_in_threadpool(store.report)

    return router
