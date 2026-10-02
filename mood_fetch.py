# -*- coding: utf-8 -*-
"""市場情緒指標：13 項資料抓取（全部免 API key）

每個抓取器回傳 [(日期 'YYYY-MM-DD', 數值, extra dict 或 None), ...]，依日期遞增。
抓取失敗丟出例外，由呼叫端記錄；不會回傳推估值。
"""
import datetime as dt
import html as _html
import io
import json
import re
import time

import requests

UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/128.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
TIMEOUT = 45
MON = {m: i for i, m in enumerate(["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


def _get(url, headers=None, **kw):
    r = requests.get(url, headers=headers or UA, timeout=TIMEOUT, **kw)
    r.raise_for_status()
    return r


def _text(h):
    h = re.sub(r"<script.*?</script>|<style.*?</style>", " ", h, flags=re.S)
    return re.sub(r"\s+", " ", _html.unescape(re.sub(r"<[^>]+>", " ", h))).replace(" ", " ")


def _iso(d):
    return d.strftime("%Y-%m-%d")


def _month_end(y, m):
    nxt = dt.date(y + (m == 12), m % 12 + 1, 1)
    return nxt - dt.timedelta(days=1)


# ───────── 短期 ─────────
def vix(since="2023-01-01"):
    """CBOE 官方 VIX 日收盤"""
    out = []
    for line in _get("https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv").text.splitlines()[1:]:
        p = line.split(",")
        if len(p) < 5:
            continue
        m, d, y = p[0].split("/")
        day = f"{y}-{int(m):02d}-{int(d):02d}"
        if day >= since:
            out.append((day, float(p[4]), None))
    return out


def cnn_fear_greed():
    """CNN 恐懼貪婪指數（近一年日資料）"""
    j = _get("https://production.dataviz.cnn.io/index/fearandgreed/graphdata").json()
    seen = {}
    for p in j.get("fear_and_greed_historical", {}).get("data", []):
        day = _iso(dt.datetime.utcfromtimestamp(p["x"] / 1000).date())
        seen[day] = (day, round(float(p["y"]), 1), {"rating": p.get("rating")})
    cur = j["fear_and_greed"]
    day = cur["timestamp"][:10]
    seen[day] = (day, round(float(cur["score"]), 1), {"rating": cur.get("rating")})
    return sorted(seen.values())


def aaii_sentiment(since="2020-01-01"):
    """AAII 散戶情緒（官方 Excel，週資料）；value＝看多 %，extra 含看空與多空差"""
    import xlrd
    wb = xlrd.open_workbook(file_contents=_get("https://www.aaii.com/files/surveys/sentiment.xls").content)
    ws = wb.sheet_by_index(0)
    out = []
    for i in range(ws.nrows):
        r = ws.row_values(i)
        if not (isinstance(r[0], float) and r[0] > 30000) or not all(isinstance(x, float) for x in r[1:4]):
            continue
        day = _iso(xlrd.xldate_as_datetime(r[0], wb.datemode).date())
        if day < since:
            continue
        bull, neu, bear = (x * 100 if x <= 1 else x for x in r[1:4])
        out.append((day, round(bull, 1), {"neutral": round(neu, 1), "bearish": round(bear, 1),
                                          "spread": round(bull - bear, 1)}))
    return out


def cboe_equity_pc(have=(), backfill_days=90):
    """CBOE 個股賣權買權比；每日一個檔案，補齊 have 裡沒有的日期"""
    out = []
    today = dt.date.today()
    span = backfill_days if len(have) < 40 else 10
    for back in range(1, span + 1):
        d = today - dt.timedelta(days=back)
        day = _iso(d)
        if d.weekday() >= 5 or day in have:
            continue
        r = requests.get(f"https://cdn.cboe.com/data/us/options/market_statistics/daily/{day}_daily_options",
                         headers=UA, timeout=TIMEOUT)
        if r.status_code != 200:
            continue                      # 休市或尚未公布
        for x in r.json().get("ratios", []):
            if x.get("name", "").upper() == "EQUITY PUT/CALL RATIO":
                out.append((day, float(x["value"]), None))
        time.sleep(0.3)
    return sorted(out)


def tw_vix(have=(), months=15):
    """臺指選擇權波動率指數（期交所每月檔，每日收盤）；過去月份已有資料就不再抓"""
    out = []
    today = dt.date.today()
    have_months = {d[:7] for d in have}
    for k in range(months):
        y, m = today.year, today.month - k
        while m <= 0:
            y, m = y - 1, m + 12
        ym = f"{y}-{m:02d}"
        if k >= 2 and ym in have_months:
            continue
        r = requests.get(f"https://www.taifex.com.tw/file/taifex/Dailydownload/vix/log2data/{y}{m:02d}new.txt",
                         headers=UA, timeout=TIMEOUT)
        if r.status_code != 200 or b"<HTML" in r.content[:200].upper():
            continue
        for line in r.content.decode("big5", "ignore").splitlines():
            p = line.split()
            if len(p) >= 3 and re.fullmatch(r"\d{8}", p[0]):
                out.append((f"{p[0][:4]}-{p[0][4:6]}-{p[0][6:]}", float(p[2]),
                            {"last_min_avg": float(p[3])} if len(p) > 3 else None))
        time.sleep(0.3)
    if not out and not have:
        raise ValueError("期交所 VIX 檔案都抓不到")
    return sorted(out)


# ───────── 中期 ─────────
def finra_margin():
    """FINRA 保證金負債（Debit Balances，百萬美元，月資料）"""
    t = _text(_get("https://www.finra.org/rules-guidance/key-topics/margin-accounts/margin-statistics",
                   headers={"User-Agent": "Mozilla/5.0"}).text)
    out = {}
    for mon, yy, debit in re.findall(r"\b([A-Z][a-z]{2})-(\d{2}) ([\d,]{7,}) [\d,]+ [\d,]+", t):
        if mon in MON:
            day = _iso(_month_end(2000 + int(yy), MON[mon]))
            out[day] = (day, float(debit.replace(",", "")), None)
    if not out:
        raise ValueError("FINRA 表格解析不到資料")
    return sorted(out.values())


def gdp():
    """美國名目 GDP（季，年化，兆美元）— multpl"""
    t = _text(_get("https://www.multpl.com/us-gdp/table/by-quarter").text)
    out = []
    for mon, d, y, v in re.findall(r"\b([A-Z][a-z]{2}) (\d{1,2}), (\d{4}) ([\d.]+) trillion", t):
        out.append((_iso(dt.date(int(y), MON[mon], int(d))), float(v), None))
    if not out:
        raise ValueError("multpl GDP 解析不到資料")
    return sorted(set(out))


def ipo_stats():
    """美股 IPO（Renaissance，今年累計）；value＝募資額（十億美元），extra 含件數與年增"""
    t = _text(_get("https://www.renaissancecapital.com/IPO-Center/Stats").text)
    extra = {}
    m = re.search(r"Total proceeds raised were \$([\d.,]+) bil this year, a ([+-]?[\d.]+)% change", t)
    if not m:
        raise ValueError("Renaissance 募資額解析失敗")
    for n, kind, chg in re.findall(r"There have been ([\d,]+) IPOs (\w+) this year, a ([+-]?[\d.]+)% change", t):
        extra[f"{kind}_count"] = int(n.replace(",", ""))
        extra[f"{kind}_yoy"] = float(chg)
    extra["proceeds_yoy"] = float(m.group(2))
    return [(_iso(dt.date.today()), float(m.group(1).replace(",", "")), extra)]


def nyse_breadth():
    """紐約證交所上漲／下跌家數（WSJ Markets Diary）；value＝上漲－下跌，extra 含新高／新低家數"""
    u = ("https://www.wsj.com/market-data/stocks/marketsdiary?id=%7B%22application%22%3A%22WSJ%22%2C%22"
         "marketsDiaryType%22%3A%22overview%22%7D&type=mdc_marketsdiary")
    data = _get(u, headers={**UA, "Accept": "application/json"}).json()["data"]
    rows = {}
    for s in data["instrumentSets"]:
        label = (s.get("headerFields") or [{}])[0].get("label", "")
        for i in s.get("instruments", []):
            if "NYSE" in i:
                rows[f"{label}|{i.get('name')}"] = int(str(i["NYSE"]).replace(",", ""))
    a, d = rows.get("Issues|Advancing"), rows.get("Issues|Declining")
    if a is None or d is None:
        raise ValueError("WSJ 漲跌家數解析失敗：" + json.dumps(data)[:300])
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", data.get("timestamp", ""))
    if not m:
        raise ValueError("WSJ 日期解析失敗：" + str(data.get("timestamp")))
    y = int(m.group(3)) + (2000 if len(m.group(3)) == 2 else 0)
    day = _iso(dt.date(y, int(m.group(1)), int(m.group(2))))
    return [(day, float(a - d), {"advancing": a, "declining": d,
                                 "new_highs": rows.get("Issues At|New Highs"), "new_lows": rows.get("Issues At|New Lows")})]


def bofa_bull_bear(have=(), limit=12):
    """美銀牛熊指標：從 Finvaulta 的 Flow Show 週報內文擷取"""
    base = "https://finvaulta.com"
    h = _get(base + "/research/bank-of-america/series/the-flow-show").text
    links = sorted(set(re.findall(r'href="(/research/bank-of-america/the-flow-show[^"]*?-(\d{4}-\d{2}-\d{2}))"', h)),
                   key=lambda x: x[1], reverse=True)
    pat = re.compile(r"Bull\s*(?:&|and)\s*Bear(?:\s*Indicator)?[^.]{0,120}?\b(\d{1,2}\.\d)\b", re.I)
    out = []
    for path, day in links[:limit]:
        if day in have:
            continue
        t = _text(_get(base + path).text)
        m = pat.search(t)
        if m and 0 <= float(m.group(1)) <= 10:
            out.append((day, float(m.group(1)), {"report": path.rsplit("/", 1)[-1]}))
        time.sleep(0.5)
    return sorted(out)


# ───────── 長期 ─────────
def _yahoo(symbol, rng, interval):
    j = _get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={rng}&interval={interval}").json()
    res = j["chart"]["result"][0]
    closes = res["indicators"]["quote"][0]["close"]
    out = {}
    for ts, c in zip(res["timestamp"], closes):
        if c is not None:
            day = _iso(dt.datetime.utcfromtimestamp(ts).date())
            out[day] = (day, round(float(c), 2), None)
    meta = res["meta"]
    if meta.get("regularMarketPrice") and meta.get("regularMarketTime"):
        day = _iso(dt.datetime.utcfromtimestamp(meta["regularMarketTime"]).date())
        out[day] = (day, round(float(meta["regularMarketPrice"]), 2), None)
    return sorted(out.values())


def wilshire5000():
    """Wilshire 5000（點數約等於美股總市值，十億美元）：月資料 10 年＋最新一日"""
    monthly = _yahoo("%5EW5000", "10y", "1mo")
    latest = _yahoo("%5EW5000", "5d", "1d")[-1:]
    return sorted({d: (d, v, e) for d, v, e in monthly + latest}.values())


def sp500():
    return _yahoo("%5EGSPC", "2y", "1d")


def cape(since="2000-01-01"):
    """席勒本益比（multpl 月表）"""
    t = _text(_get("https://www.multpl.com/shiller-pe/table/by-month").text)
    out = {}
    for mon, d, y, v in re.findall(r"\b([A-Z][a-z]{2}) (\d{1,2}), (\d{4}) ([\d.]+)\b", t):
        day = _iso(dt.date(int(y), MON[mon], int(d)))
        if day >= since:
            out.setdefault(day, (day, float(v), None))
    if not out:
        raise ValueError("multpl CAPE 解析不到資料")
    return sorted(out.values())


def treasury_10y2y():
    """美國財政部每日殖利率曲線：10 年－2 年（百分點）"""
    out = []
    y = dt.date.today().year
    for year in (y - 1, y):
        u = (f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/"
             f"{year}/all?type=daily_treasury_yield_curve&field_tdr_date_value={year}&page&_format=csv")
        lines = _get(u).text.splitlines()
        head = [h.strip('"') for h in lines[0].split(",")]
        i2, i10 = head.index("2 Yr"), head.index("10 Yr")
        for line in lines[1:]:
            p = line.split(",")
            if len(p) <= i10 or not p[i2] or not p[i10]:
                continue
            m, d, yy = p[0].split("/")
            out.append((f"{yy}-{m}-{d}", round(float(p[i10]) - float(p[i2]), 2),
                        {"y2": float(p[i2]), "y10": float(p[i10])}))
    return sorted(out)


def lei():
    """美國經濟諮商理事會領先指標（新聞稿文字）；value＝指數，extra 含月變化與近 6 個月變化"""
    t = _text(_get("https://www.conference-board.org/topics/us-leading-indicators").text)
    m = re.search(r"Leading Economic Index[®\s]*\(LEI\) for the U\.?S\.? (increased|decreased|rose|fell|declined|was unchanged)"
                  r"(?: by)? ([\d.]+)?%? ?(?:percent )?in (\w+) (\d{4})(?:,)? to ([\d.]+)", t)
    if not m:
        raise ValueError("LEI 新聞稿解析失敗：" + t[t.find("(LEI) for the U"):][:300])
    sign = -1 if m.group(1) in ("decreased", "fell", "declined") else 1
    month = MON.get(m.group(3)[:3])
    day = _iso(_month_end(int(m.group(4)), month))
    extra = {"mom": sign * float(m.group(2) or 0)}
    s = re.search(r"six-month growth (?:rate )?was ([\u2013\u2212\-+]?)([\d.]+)%", t)
    if s:
        extra["six_month"] = (-1 if s.group(1) in ("\u2013", "\u2212", "-") else 1) * float(s.group(2))
    return [(day, float(m.group(5)), extra)]


FETCHERS = {
    "vix": vix, "tw_vix": tw_vix, "cnn_fg": cnn_fear_greed, "aaii": aaii_sentiment, "put_call": cboe_equity_pc,
    "margin": finra_margin, "gdp": gdp, "ipo": ipo_stats, "nyse_ad": nyse_breadth, "bofa": bofa_bull_bear,
    "w5000": wilshire5000, "spx": sp500, "cape": cape, "t10y2y": treasury_10y2y, "lei": lei,
}
NEEDS_HAVE = {"put_call", "bofa", "tw_vix"}
