"""主動式 ETF 日報 PDF

流程：
1. 程式從資料庫與證交所／櫃買盤後資料算出「事實清單」（數字都在這一層決定）
2. 把事實清單接上提示詞（templates/report_prompt.txt）交給 Gemini 寫說明
3. 檢查說明裡的每個數字都能在事實清單找到；對不上的段落改用程式模板句子
4. 用 WeasyPrint 把 templates/report_pdf.html 轉成 PDF

環境變數：
  GEMINI_API_KEY          沒設就全部用程式模板句子
  GEMINI_MODEL            預設 gemini-3.1-pro-preview
  GEMINI_FALLBACK_MODEL   主模型失敗時改用，預設 gemini-3.8-flash
"""
import datetime as dt
import hashlib
import html as H
import json
import logging
import os
import re
import threading
import time

import requests

logger = logging.getLogger(__name__)

KEY = os.getenv("GEMINI_API_KEY", "").strip()
MODELS = [m for m in (os.getenv("GEMINI_MODEL", "gemini-3.1-pro-preview").strip(),
                      os.getenv("GEMINI_FALLBACK_MODEL", "gemini-3.8-flash").strip()) if m]
HERE = os.path.dirname(os.path.abspath(__file__))
UA = {"User-Agent": "Mozilla/5.0 (etf-monitor report)"}
TPEX_IND = {"01": "水泥工業", "02": "食品工業", "03": "塑膠工業", "04": "紡織纖維", "05": "電機機械", "06": "電器電纜",
            "08": "玻璃陶瓷", "10": "鋼鐵工業", "11": "橡膠工業", "12": "汽車工業", "14": "建材營造", "15": "航運業",
            "16": "觀光餐旅", "17": "金融保險", "18": "貿易百貨", "20": "其他業", "21": "化學工業", "22": "生技醫療業",
            "23": "油電燃氣業", "24": "半導體業", "25": "電腦及週邊設備業", "26": "光電業", "27": "通信網路業",
            "28": "電子零組件業", "29": "電子通路業", "30": "資訊服務業", "31": "其他電子業", "32": "文化創意業",
            "33": "農業科技業", "35": "綠能環保", "36": "數位雲端", "37": "運動休閒", "38": "居家生活"}
zh = lambda sh: round((sh or 0) / 1000)
MAX_DAYS = int(os.getenv("REPORT_MAX_DAYS", "10"))     # 報告最多可回看幾個資料日


def etf_style(name):
    """依名稱關鍵字分 ETF 類型"""
    if re.search(r"升級50|增強50|未來50", name or ""):
        return "大盤增強50型"
    if re.search(r"高息|高股息|收益|策略高息|趨勢優選", name or ""):
        return "高息收益型"
    return "成長科技型"


def etf_short(name, code):
    n = re.sub(r"主動式\s*ETF$|ETF$", "", (name or code).strip())
    return re.sub(r"^主動", "", n).strip() or code


# ---------------------------------------------------------------- 外部資料
class Market:
    """證交所／櫃買盤後資料（加權指數、漲跌家數、成交金額、三大法人金額、個股漲跌）＋股票簡稱與產業別"""

    def __init__(self, db):
        self.db = db
        self._day = {}
        self._meta = None
        self._meta_at = 0
        self.lock = threading.Lock()
        self._q("CREATE TABLE IF NOT EXISTS stock_meta (code TEXT PRIMARY KEY, short_name TEXT, industry TEXT, market TEXT, updated TEXT)")

    @property
    def ph(self):
        return "%s" if self.db.db_type == "postgresql" else "?"

    def _q(self, sql, params=(), fetch="none"):
        return self.db.execute_query(sql.replace("?", self.ph), params, fetch)

    @staticmethod
    def _get(url, **kw):
        r = requests.get(url, headers=UA, timeout=40, **kw)
        r.raise_for_status()
        return r

    def meta(self):
        """代號 -> {short, industry, market}；資料庫有就用，超過 7 天重抓證交所 ISIN 清單"""
        if self._meta is not None and time.time() - self._meta_at < 3600:
            return self._meta
        rows = self._q("SELECT code, short_name, industry, market, updated FROM stock_meta", fetch="all") or []
        stale = not rows or min(r["updated"] or "" for r in rows) < (dt.date.today() - dt.timedelta(days=7)).isoformat()
        if stale:
            try:
                fresh = {}
                for mode, mkt in ((2, "上市"), (4, "上櫃")):
                    r = self._get(f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}")
                    r.encoding = "big5"
                    for m in re.finditer(r"<tr><td bgcolor=#FAFAD2>([0-9A-Z]{4,6})　([^<]+)</td>(?:<td[^>]*>[^<]*</td>){3}<td[^>]*>([^<]*)</td>", r.text):
                        fresh[m.group(1)] = (m.group(2).strip(), m.group(3).strip(), mkt)
                if fresh:
                    today = dt.date.today().isoformat()
                    for code, (s, ind, mkt) in fresh.items():
                        self._q("INSERT INTO stock_meta (code, short_name, industry, market, updated) VALUES (?, ?, ?, ?, ?) "
                                "ON CONFLICT (code) DO UPDATE SET short_name = EXCLUDED.short_name, industry = EXCLUDED.industry, "
                                "market = EXCLUDED.market, updated = EXCLUDED.updated", (code, s, ind, mkt, today))
                    rows = [{"code": c, "short_name": v[0], "industry": v[1], "market": v[2]} for c, v in fresh.items()]
                    logger.info(f"[report] 股票簡稱／產業別更新 {len(fresh)} 檔")
            except Exception as e:
                logger.warning(f"[report] 抓股票簡稱／產業別失敗（沿用舊資料）：{e}")
        self._meta = {r["code"]: {"short": r["short_name"], "industry": r["industry"], "market": r["market"]} for r in rows}
        self._meta_at = time.time()
        return self._meta

    def day(self, date):
        """date 'YYYY-MM-DD' 的大盤與個股漲跌；失敗回傳空 dict"""
        if date in self._day:
            return self._day[date]
        ymd = date.replace("-", "")
        out = {"price": {}}
        T = "https://www.twse.com.tw/rwd/zh"
        num = lambda s: float(str(s).replace(",", "") or 0)
        try:
            a = self._get(f"{T}/afterTrading/MI_INDEX?date={ymd}&type=ALLBUT0999&response=json").json()
            tables = a.get("tables") or []
            for t in tables:
                title, data = t.get("title") or "", t.get("data") or []
                if "價格指數(臺灣證券交易所)" in title:
                    idx = next((r for r in data if r[0] == "發行量加權股價指數"), None)
                    if idx:
                        sign = -1 if "green" in idx[2] else 1
                        out["index"] = {"close": num(idx[1]), "chg": sign * num(idx[3]), "pct": num(idx[4])}
                elif title.startswith("漲跌證券數"):
                    out["breadth"] = {r[0]: r[2] for r in data}
                elif "每日收盤行情" in title:
                    f = t["fields"]
                    ic, iclose, isign, idiff = f.index("證券代號"), f.index("收盤價"), f.index("漲跌(+/-)"), f.index("漲跌價差")
                    for r in data:
                        try:
                            close, diff = num(r[iclose]), num(r[idiff])
                            sign = -1 if "green" in r[isign] else 1
                            prev = close - sign * diff
                            out["price"][r[ic]] = round(sign * diff / prev * 100, 2) if prev else 0.0
                        except (ValueError, ZeroDivisionError):
                            pass
        except Exception as e:
            logger.warning(f"[report] 證交所 MI_INDEX 失敗：{e}")
        try:
            q = self._get(f"{T}/afterTrading/FMTQIK?date={ymd}&response=json").json()
            roc = f"{int(ymd[:4]) - 1911}/{ymd[4:6]}/{ymd[6:]}"
            row = next((r for r in q.get("data") or [] if r[0] == roc), None)
            if row:
                out["turnover_yi"] = round(num(row[2]) / 1e8)
        except Exception as e:
            logger.warning(f"[report] 證交所 FMTQIK 失敗：{e}")
        try:
            b = self._get(f"{T}/fund/BFI82U?type=day&dayDate={ymd}&response=json").json()
            v = {r[0]: num(r[3]) for r in b.get("data") or []}
            if v:
                yi = lambda x: round(x / 1e8, 2)
                out["inst_yi"] = {"外資及陸資": yi(v.get("外資及陸資(不含外資自營商)", 0)), "投信": yi(v.get("投信", 0)),
                                  "自營商": yi(v.get("自營商(自行買賣)", 0) + v.get("自營商(避險)", 0)), "合計": yi(v.get("合計", 0))}
        except Exception as e:
            logger.warning(f"[report] 證交所 BFI82U 失敗：{e}")
        try:   # 櫃買上櫃股票收盤行情（新版網站要用 POST）
            r = requests.post("https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes", headers=UA, timeout=60,
                              data={"date": f"{ymd[:4]}/{ymd[4:6]}/{ymd[6:]}", "id": "", "response": "json"})
            r.raise_for_status()
            j = r.json()
            if str(j.get("date")) == ymd:
                t = (j.get("tables") or [{}])[0]
                f = t.get("fields") or []
                ic, iclose, ichg = f.index("代號"), f.index("收盤"), f.index("漲跌")
                for row in t.get("data") or []:
                    try:
                        close, chg = num(row[iclose]), num(str(row[ichg]).strip().replace("+", ""))
                        prev = close - chg
                        out["price"].setdefault(row[ic], round(chg / prev * 100, 2) if prev else 0.0)
                    except (ValueError, ZeroDivisionError):
                        pass
        except Exception as e:
            logger.warning(f"[report] 櫃買收盤行情失敗：{e}")
        if out.get("index"):
            self._day[date] = out
        return out


# ---------------------------------------------------------------- 事實清單
class Report:
    def __init__(self, db, dbq, in_scope, inst=None):
        """db: database_config；dbq: DatabaseQuery；in_scope(scope, fn, *args) 在指定日報範圍內執行；inst: inst_flow.flow"""
        self.db, self.dbq, self.in_scope, self.inst = db, dbq, in_scope, inst
        self.market = Market(db)
        self.locks = {}
        self._pdf = {}
        self.last = {}
        self._q("CREATE TABLE IF NOT EXISTS etf_report (d TEXT NOT NULL, scope TEXT NOT NULL, fp TEXT, model TEXT, "
                "facts TEXT, ai TEXT, notes TEXT, created TEXT, PRIMARY KEY (d, scope))")

    _q = Market._q
    ph = Market.ph

    # ---- 讀資料（呼叫端已設好範圍）
    def _moves(self, date):
        return self.dbq.execute_query(
            f"SELECT etf_code, stock_code, stock_name, change_type, old_shares, new_shares FROM holdings_changes "
            f"WHERE change_date = {self.dbq._get_placeholder()} AND {self.dbq._scope_sql('etf_code')}", (date,), fetch="all") or []

    def fingerprint(self, date, scope):
        """資料有變（晚一點的排程補抓到更多 ETF）就重寫"""
        def f():
            ph = self.dbq._get_placeholder()
            a = self.dbq.execute_query(f"SELECT COUNT(*) AS n, COUNT(DISTINCT etf_code) AS e FROM etf_holdings "
                                       f"WHERE update_date = {ph} AND {self.dbq._scope_sql('etf_code')}", (date,), fetch="one") or {}
            b = self.dbq.execute_query(f"SELECT COUNT(*) AS n, COALESCE(SUM(new_shares - old_shares), 0) AS s FROM holdings_changes "
                                       f"WHERE change_date = {ph} AND {self.dbq._scope_sql('etf_code')}", (date,), fetch="one") or {}
            return f'{a.get("n")}-{a.get("e")}-{b.get("n")}-{b.get("s")}'
        return self.in_scope(scope, f)

    def build_facts(self, date, scope):
        return self.in_scope(scope, self._build_facts, date, scope)

    def _build_facts(self, date, scope):
        dbq = self.dbq
        meta = self.market.meta()
        mk = self.market.day(date)
        price = mk.get("price", {})
        inst = {}
        if self.inst:
            try:
                inst = {r["id"]: r for r in (self.inst.compute(date.replace("-", "")) or {}).get("rows", [])}
            except Exception as e:
                logger.warning(f"[report] 三大法人資料讀取失敗：{e}")
        names = dbq.etf_names
        S = lambda c, n="": (meta.get(c) or {}).get("short") or (inst.get(c) or {}).get("name") or n
        ind = lambda c: (meta.get(c) or {}).get("industry") or ("公司債" if not re.fullmatch(r"\d{4}", c or "") else "")
        eshort = lambda e: etf_short(names.get(e, e), e)

        day = dbq.get_etf_day(date)
        moves = self._moves(date)
        buy = sum(max(0, (m["new_shares"] or 0) - (m["old_shares"] or 0)) for m in moves)
        sell = sum(max(0, (m["old_shares"] or 0) - (m["new_shares"] or 0)) for m in moves)
        per = {}
        for m in moves:
            c, o, n = m["stock_code"], m["old_shares"] or 0, m["new_shares"] or 0
            p = per.setdefault(c, {"代號": c, "簡稱": S(c, m["stock_name"]), "加": 0, "減": 0, "加ETF": set(), "減ETF": set(),
                                   "新進ETF": set(), "出清ETF": set()})
            if n > o:
                p["加"] += n - o; p["加ETF"].add(m["etf_code"])
            elif o > n:
                p["減"] += o - n; p["減ETF"].add(m["etf_code"])
            if m["change_type"] == "NEW": p["新進ETF"].add(m["etf_code"])
            if m["change_type"] == "REMOVED": p["出清ETF"].add(m["etf_code"])

        trend_cache = {}
        def trend(c):
            if c not in trend_cache:
                # 補產舊日期時，只取該日（含）以前的 30 個資料日
                trend_cache[c] = [t for t in dbq.get_stock_trend(c, 45) if t["date"] <= date][-30:]
            return trend_cache[c]
        def streak(c, sign):
            n = 0
            for r in reversed(trend(c)):
                if (r["net"] or 0) * sign > 0: n += 1
                else: break
            return n or None

        def row(p, side):
            c, i = p["代號"], inst.get(p["代號"], {})
            return {"代號": c, "簡稱": p["簡稱"], "產業別": ind(c), "淨增減張": zh(p["加"] - p["減"]),
                    "加碼張": zh(p["加"]), "減碼張": zh(p["減"]),
                    "加碼ETF": [f"{eshort(e)}（{e}）" for e in sorted(p["加ETF"])], "減碼ETF": [f"{eshort(e)}（{e}）" for e in sorted(p["減ETF"])],
                    "新進ETF": sorted(p["新進ETF"]), "出清ETF": sorted(p["出清ETF"]),
                    "股價漲跌幅%": price.get(c), "投信當日買賣超張": i.get("trust"), "外資當日買賣超張": i.get("foreign"),
                    "連續同向天數": streak(c, 1 if side == "buy" else -1)}

        ranked = sorted(per.values(), key=lambda p: p["加"] - p["減"])
        top_buy = [row(p, "buy") for p in reversed(ranked) if p["加"] > p["減"]][:15]
        top_sell = [row(p, "sell") for p in ranked if p["減"] > p["加"]][:15]
        both = [{"簡稱": p["簡稱"], "代號": p["代號"], "加碼張": zh(p["加"]), "減碼張": zh(p["減"]), "淨增減張": zh(p["加"] - p["減"])}
                for p in per.values() if zh(p["加"]) >= 50 and zh(p["減"]) >= 50]
        net = buy - sell
        big = max(per.values(), key=lambda p: abs(p["加"] - p["減"]), default=None)
        big_net = (big["加"] - big["減"]) if big else 0

        suspect = []
        for m in moves:
            o, n = m["old_shares"] or 0, m["new_shares"] or 0
            if m["change_type"] == "INCREASED" and o > 0 and n / o >= 1.8 and zh(n - o) >= 200:
                i = inst.get(m["stock_code"], {})
                if (i.get("trust") or 0) <= 0 and (i.get("foreign") or 0) <= 0:
                    suspect.append({"ETF": m["etf_code"], "簡稱": S(m["stock_code"], m["stock_name"]), "代號": m["stock_code"],
                                    "前張": zh(o), "後張": zh(n), "倍數": round(n / o, 2)})

        etf_rows = []
        for r in day:
            mv = [m for m in moves if m["etf_code"] == r["code"]]
            etf_rows.append({"代號": r["code"], "簡稱": eshort(r["code"]), "類型": etf_style(r["name"]),
                             "當日漲跌%": r["change_pct"], "折溢價%": r["premium_pct"], "持股檔數": r["holdings"],
                             "新進": r["new"], "加碼": r["inc"], "減碼": r["dec"], "出清": r["removed"],
                             "買進張": zh(sum(max(0, (m["new_shares"] or 0) - (m["old_shares"] or 0)) for m in mv)),
                             "賣出張": zh(sum(max(0, (m["old_shares"] or 0) - (m["new_shares"] or 0)) for m in mv))})
        etf_rows.sort(key=lambda r: -(r["買進張"] + r["賣出張"]))
        style = {}
        for r in etf_rows:
            s = style.setdefault(r["類型"], {"ETF數": 0, "買進張": 0, "賣出張": 0})
            s["ETF數"] += 1; s["買進張"] += r["買進張"]; s["賣出張"] += r["賣出張"]
        for s in style.values():
            s["淨額張"] = s["買進張"] - s["賣出張"]

        new_stocks = sorted([{"代號": p["代號"], "簡稱": p["簡稱"], "產業別": ind(p["代號"]),
                              "新進ETF": [f"{eshort(e)}（{e}，{etf_style(names.get(e, ''))}）" for e in sorted(p["新進ETF"])],
                              "合計買進張": zh(p["加"]), "股價漲跌幅%": price.get(p["代號"]),
                              "投信當日買賣超張": inst.get(p["代號"], {}).get("trust"), "外資當日買賣超張": inst.get(p["代號"], {}).get("foreign")}
                             for p in per.values() if p["新進ETF"]], key=lambda x: -x["合計買進張"])
        fb = dbq.get_first_buys(date)
        first = [{"代號": r["stock_code"], "簡稱": S(r["stock_code"], r["stock_name"]), "全新面孔": r["first_any"],
                  "買進ETF": [f'{eshort(e["etf_code"])}（{e["etf_code"]}，{zh(e["shares"])}張）' for e in r["etfs"]]} for r in fb["rows"]]

        cross = sorted(dbq.get_cross_holdings(date), key=lambda r: (-r["etf_count"], -r["total_shares"]))
        core = cross[:8]
        cross_top = []
        for r in cross[:30]:
            c = r["stock_code"]
            cross_top.append({"代號": c, "簡稱": S(c, r["stock_name"]), "持有ETF數": r["etf_count"], "合計持股張": zh(r["total_shares"]),
                              "加碼張": zh(r["total_increase"]), "減碼張": zh(r["total_decrease"]),
                              "淨增減張": zh(r["total_increase"] - r["total_decrease"]),
                              "_trend": [[t["shares"] or 0, t["net"] or 0] for t in trend(c)]})

        idx = mk.get("index") or {}
        br = mk.get("breadth") or {}
        top10 = top_buy[:10]
        facts = {
            "資料日": date, "範圍": f"{'積極型' if scope == 'aggr' else '不分類'}（{len(day)} 檔主動式 ETF）",
            "大盤": {"加權指數收盤": idx.get("close"), "漲跌點": idx.get("chg"), "漲跌幅%": idx.get("pct"),
                   "成交金額億元": mk.get("turnover_yi"), "上漲家數": br.get("上漲(漲停)"), "下跌家數": br.get("下跌(跌停)")} if idx else None,
            "三大法人買賣超億元": mk.get("inst_yi"),
            "主動ETF總覽": {"合計加碼張": zh(buy), "合計減碼張": zh(sell), "淨額張": zh(net),
                         "新進筆數": sum(r["new"] for r in day), "加碼筆數": sum(r["inc"] for r in day),
                         "減碼筆數": sum(r["dec"] for r in day), "出清筆數": sum(r["removed"] for r in day),
                         "跨ETF共同持有檔數": len(cross), "當日有異動的ETF數": sum(1 for r in etf_rows if r["買進張"] or r["賣出張"]),
                         "有持股資料的ETF數": sum(1 for r in day if r["holdings"])},
            "集中度": {"淨額最大單一股票": f'{big["簡稱"]}（{big["代號"]}）' if big else None, "該股淨增減張": zh(big_net),
                    "佔主動ETF淨額比例%": round(big_net / net * 100) if net else None, "扣除該股後淨額張": zh(net - big_net)},
            "加碼前15": top_buy, "減碼前15": top_sell, "加減碼互見": both, "疑似除權或面額變更": suspect,
            "首次買進（該ETF過去從未持有）": {"起算日": fb.get("since"), "股票": first},
            "新進股票": new_stocks, "各ETF動作": etf_rows, "依ETF類型彙總": style,
            "核心持股（最多ETF共同持有）": [{"簡稱": S(r["stock_code"], r["stock_name"]), "代號": r["stock_code"],
                                    "持有ETF數": r["etf_count"], "合計持股張": zh(r["total_shares"])} for r in core],
            "投信對照": {"加碼前10中投信當日也買超的檔數": sum(1 for r in top10 if (r["投信當日買賣超張"] or 0) > 0),
                      "加碼前10中外資當日也買超的檔數": sum(1 for r in top10 if (r["外資當日買賣超張"] or 0) > 0),
                      "說明": "主動式ETF由投信發行，其買賣包含在投信買賣超之內"},
            "_cross": cross_top,       # 底線開頭的欄位只給 PDF 用，不送 AI
        }
        return facts

    # ---- 寫說明
    PROMPT = "report_prompt.txt"
    TAG = "report"

    def fallback(self, facts):
        return fallback_text(facts)

    def render(self, rep, templates):
        return render_html(rep, templates)

    def prompt(self, facts):
        tpl = open(os.path.join(HERE, "templates", self.PROMPT), encoding="utf-8").read()
        clean = {k: v for k, v in facts.items() if not k.startswith("_")}
        return tpl.replace("{facts}", json.dumps(clean, ensure_ascii=False, indent=1))

    @staticmethod
    def _gemini(model, prompt):
        r = requests.post(f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
                          headers={"x-goog-api-key": KEY, "Content-Type": "application/json"}, timeout=240,
                          json={"contents": [{"role": "user", "parts": [{"text": prompt}]}],
                                "generationConfig": {"responseMimeType": "application/json"}})
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
        parts = (((r.json().get("candidates") or [{}])[0].get("content") or {}).get("parts") or [])
        text = "".join(p.get("text", "") for p in parts if not p.get("thought"))
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip())
        return json.loads(text)

    def write(self, facts):
        """回傳 (ai, model, notes)；notes 記錄哪些段落被換成模板句子"""
        tpl = self.fallback(facts)
        notes = []
        if not KEY:
            return tpl, "template", ["未設定 GEMINI_API_KEY，全部使用程式模板"]
        p = self.prompt(facts)
        for model in MODELS:
            try:
                ai = self._gemini(model, p)
                ai, fixed = merge_checked(facts, ai, tpl)
                if fixed:
                    notes.append(f"{model}：{len(fixed)} 處數字對不上，改用模板：{'、'.join(fixed[:8])}")
                return ai, model, notes
            except Exception as e:
                notes.append(f"{model} 失敗：{str(e)[:200]}")
                logger.warning(f"[report] {model} 失敗：{e}")
        return tpl, "template", notes

    # ---- 對外
    def stored(self, date, scope):
        """只讀資料庫裡已產生的版本（不呼叫 AI）；沒有就回 None"""
        row = self._q("SELECT fp, model, facts, ai, notes, created FROM etf_report WHERE d = ? AND scope = ?", (date, scope), fetch="one")
        if not row:
            return None
        return {"fp": row["fp"], "facts": json.loads(row["facts"]), "ai": json.loads(row["ai"]), "model": row["model"],
                "notes": json.loads(row["notes"] or "[]"), "created": row["created"]}

    def get(self, date, scope, refresh=False):
        """取得（必要時產生）報告；同一天同一範圍資料沒變就沿用資料庫裡的版本"""
        lock = self.locks.setdefault((date, scope), threading.Lock())
        with lock:
            fp = self.fingerprint(date, scope)
            if not refresh:
                row = self.stored(date, scope)
                if row and row["fp"] == fp:
                    return row
            t0 = time.time()
            facts = self.build_facts(date, scope)
            ai, model, notes = self.write(facts)
            created = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M")
            self._q("INSERT INTO etf_report (d, scope, fp, model, facts, ai, notes, created) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT (d, scope) DO UPDATE SET fp = EXCLUDED.fp, model = EXCLUDED.model, facts = EXCLUDED.facts, "
                    "ai = EXCLUDED.ai, notes = EXCLUDED.notes, created = EXCLUDED.created",
                    (date, scope, fp, model, json.dumps(facts, ensure_ascii=False), json.dumps(ai, ensure_ascii=False),
                     json.dumps(notes, ensure_ascii=False), created))
            logger.info(f"[{self.TAG}] {date} {scope} 產生完成（{model}，{time.time() - t0:.0f} 秒）{'；' + '；'.join(notes) if notes else ''}")
            return {"facts": facts, "ai": ai, "model": model, "notes": notes, "created": created}

    def pdf(self, date, scope, templates, refresh=False):
        rep = self.get(date, scope, refresh)
        key = (date, scope, hashlib.md5(json.dumps([rep["created"], rep["model"], rep["ai"]], ensure_ascii=False, sort_keys=True).encode()).hexdigest())
        if key not in self._pdf:
            from weasyprint import HTML
            html = self.render(rep, templates)
            self._pdf = {k: v for k, v in self._pdf.items() if k[:2] != (date, scope)}
            self._pdf[key] = HTML(string=html, base_url=HERE).write_pdf()
        return self._pdf[key]

    def run_daily(self):
        """排程用：替最新資料日產生兩種範圍的報告"""
        dates = self.dbq.get_available_dates()
        if not dates:
            return "沒有資料"
        out = []
        for scope in ("aggr", "all"):
            r = self.get(dates[0], scope)
            out.append(f"{scope}:{r['model']}")
        return f"{dates[0]} " + "、".join(out)


# ---------------------------------------------------------------- 數字檢查與模板句子
def _nums(o, out):
    if isinstance(o, dict):
        for k, v in o.items():
            if not str(k).startswith("_"):
                _nums(k, out); _nums(v, out)
    elif isinstance(o, list):
        for v in o: _nums(v, out)
    elif isinstance(o, bool) or o is None:
        pass
    elif isinstance(o, (int, float)):
        out.add(round(abs(float(o)), 2))
    elif isinstance(o, str):
        for m in re.findall(r"\d[\d,]*\.?\d*", re.sub(r"\d{4,6}[A-Z]", "", o)):
            out.add(round(float(m.replace(",", "")), 2))
    return out


def bad_numbers(facts, text):
    ok = _nums(facts, set()) | {float(i) for i in range(0, 21)}
    bad = []
    for m in re.findall(r"(?<![A-Za-z\d])\d[\d,]*\.?\d*", re.sub(r"\d{4,6}[A-Z]", "", text or "")):
        try:
            if round(float(m.replace(",", "")), 2) not in ok:
                bad.append(m)
        except ValueError:
            pass
    return bad


def merge_checked(facts, ai, tpl):
    """逐段檢查 AI 寫的數字；對不上的段落／項目換成模板句子"""
    out, fixed = {}, []
    for k, v in tpl.items():
        a = ai.get(k) if isinstance(ai, dict) else None
        if isinstance(v, list):
            items = a if isinstance(a, list) and a else v
            res = []
            for it in items:
                txt = it.get("text", "") if isinstance(it, dict) else ""
                if not isinstance(it, dict) or not txt or bad_numbers(facts, txt):
                    if isinstance(it, dict) and it.get("code"):
                        alt = next((x for x in v if x.get("code") == it.get("code")), None)
                        if alt: res.append(alt); fixed.append(f"{k}:{it.get('code')}")
                    else:
                        fixed.append(k)
                    continue
                res.append(it)
            out[k] = res or v
        else:
            if isinstance(a, str) and a.strip() and not bad_numbers(facts, a):
                out[k] = a.strip()
            else:
                out[k] = v; fixed.append(k)
    return out, fixed


def fallback_text(f):
    """沒有 AI 或 AI 寫錯時用的程式模板句子"""
    o, c = f["主動ETF總覽"], f["集中度"]
    sgn = lambda v: "買" if v >= 0 else "賣"
    tb, ts = f["加碼前15"], f["減碼前15"]
    m, inst = f.get("大盤"), f.get("三大法人買賣超億元")
    head = f"主動 ETF 淨{sgn(o['淨額張'])}超 {abs(o['淨額張']):,} 張" + (f"，{tb[0]['簡稱']}加碼居冠" if tb else "")
    bullets = []
    if tb: bullets.append({"type": "up", "text": f"{tb[0]['簡稱']}（{tb[0]['代號']}）淨增 {tb[0]['淨增減張']:,} 張居冠"})
    if ts: bullets.append({"type": "down", "text": f"{ts[0]['簡稱']}（{ts[0]['代號']}）淨減 {abs(ts[0]['淨增減張']):,} 張最多"})
    if c.get("佔主動ETF淨額比例%") and c["佔主動ETF淨額比例%"] >= 30:
        rest = c["扣除該股後淨額張"]
        bullets.append({"type": "warn", "text": f"{c['淨額最大單一股票']}佔淨額 {c['佔主動ETF淨額比例%']}%，扣除後淨{sgn(rest)}超 {abs(rest):,} 張"})
    core = f["核心持股（最多ETF共同持有）"][:4]
    if core: bullets.append({"type": "core", "text": "、".join(x["簡稱"] for x in core) + " 為最多 ETF 共同持有的核心持股"})
    if m and m.get("加權指數收盤"):
        bullets.append({"type": "info", "text": f"加權指數收 {m['加權指數收盤']:,.2f} 點，漲跌 {m['漲跌點']:+,.2f} 點"})
    ov = (f"加權指數收 {m['加權指數收盤']:,.2f} 點，漲跌 {m['漲跌點']:+,.2f} 點。" if m and m.get("加權指數收盤") else "")
    if inst: ov += f"三大法人合計買賣超 {inst['合計']:+,.2f} 億元，投信 {inst['投信']:+,.2f} 億元。"
    ov += f"主動 ETF 合計加碼 {o['合計加碼張']:,} 張、減碼 {o['合計減碼張']:,} 張，淨{sgn(o['淨額張'])}超 {abs(o['淨額張']):,} 張。"
    def sline(r):
        side = "增" if r["淨增減張"] > 0 else "減"
        n = len(r["加碼ETF"] if r["淨增減張"] > 0 else r["減碼ETF"])
        t = f"{r['產業別'] or '個股'}，{n} 檔 ETF 淨{side} {abs(r['淨增減張']):,} 張"
        if r.get("投信當日買賣超張") is not None:
            t += f"；投信{'買' if r['投信當日買賣超張'] >= 0 else '賣'}超 {abs(r['投信當日買賣超張']):,} 張"
        return {"code": r["代號"], "text": t + "。"}
    st = f["依ETF類型彙總"]
    def grp(rows):
        g = list(dict.fromkeys(r["產業別"] for r in rows[:8] if r["產業別"]))
        return "、".join(g[:4]) if g else "、".join(r["簡稱"] for r in rows[:4]) or "無"
    return {
        "headline": head, "bullets": bullets, "overview": ov,
        "sectors": f"加碼以{grp(tb)}為主；減碼以{grp(ts)}為主。",
        "stocks": [sline(r) for r in tb[:5] + ts[:3]],
        "new_stocks": [{"code": r["代號"], "text": f"主要業務為{r['產業別'] or '其他'}產業。由 {'、'.join(r['新進ETF'])} 新進 {r['合計買進張']:,} 張。"}
                       for r in f["新進股票"][:12]],
        "etf_moves": "；".join(f"{k} {v['ETF數']} 檔淨{sgn(v['淨額張'])}超 {abs(v['淨額張']):,} 張" for k, v in st.items()) + "。",
        "trust_vs_etf": f"加碼前 10 名中，投信當日也買超的有 {f['投信對照']['加碼前10中投信當日也買超的檔數']} 檔，"
                        f"外資也買超的有 {f['投信對照']['加碼前10中外資當日也買超的檔數']} 檔。",
        "outlook": "以上為當日籌碼變化整理，請搭配個股基本面與股價位置自行判斷。",
    }


# ---------------------------------------------------------------- PDF
def spark(rows, w=118, h=17):
    """近 30 日合計持股折線＋每日淨進出長條（同一條日期軸）"""
    if not rows:
        return ""
    sh = [r[0] for r in rows]; nt = [r[1] for r in rows]
    med = sorted(sh)[len(sh) // 2]
    sh = [None if (med and v < med * .5 and 0 < i < len(sh) - 1) else v for i, v in enumerate(sh)]
    ok = [v for v in sh if v is not None]
    lo, hi = min(ok), max(ok); rng = (hi - lo) or 1; bw = w / len(rows)
    mx = max(1, max(abs(x) for x in nt)); mid = h / 2
    bars = "".join(f'<rect x="{i * bw + bw * .15:.1f}" y="{(mid - v / mx * mid * .9) if v > 0 else mid:.1f}" width="{bw * .7:.1f}" '
                   f'height="{abs(v) / mx * mid * .9:.1f}" fill="{"#e5383b" if v > 0 else "#12a150"}" opacity=".55"/>' for i, v in enumerate(nt) if v)
    pts = " ".join(f"{i * bw + bw / 2:.1f},{h - 2 - (v - lo) / rng * (h - 4):.1f}" for i, v in enumerate(sh) if v is not None)
    return f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}">{bars}<polyline points="{pts}" fill="none" stroke="#2b6cb0" stroke-width="1.3"/></svg>'


def render_html(rep, templates):
    f, ai = rep["facts"], rep["ai"]
    env = templates.env
    env.filters.setdefault("n", lambda v: "—" if v is None else f"{v:,.0f}")
    env.filters.setdefault("pct", lambda v: "—" if v is None else f"{v:+.2f}%")
    tb, ts = f["加碼前15"], f["減碼前15"]
    lookup = {r["代號"]: r for r in tb + ts}
    return env.get_template("report_pdf.html").render(
        f=f, ai=ai, created=rep["created"], model=rep["model"],
        cross=[dict(r, spark=spark(r["_trend"])) for r in f["_cross"]],
        stocks=[(lookup[s["code"]], s["text"]) for s in ai["stocks"] if s.get("code") in lookup],
        new=[(next(r for r in f["新進股票"] if r["代號"] == s["code"]), s["text"]) for s in ai["new_stocks"]
             if any(r["代號"] == s.get("code") for r in f["新進股票"])],
        first_codes={r["代號"] for r in f["首次買進（該ETF過去從未持有）"]["股票"]},
        mx_b=max([r["淨增減張"] for r in tb] or [1]), mx_s=max([-r["淨增減張"] for r in ts] or [1]))


# ---------------------------------------------------------------- 掛到 FastAPI
report = None


def init(db, dbq, in_scope, inst=None):
    global report
    try:
        report = Report(db, dbq, in_scope, inst)
    except Exception as e:
        logger.error(f"❌ 日報 PDF 初始化失敗：{e}")
    return report


def create_router(templates, check_authentication):
    from fastapi import APIRouter, HTTPException, Query, Request
    from fastapi.responses import JSONResponse, Response
    from starlette.concurrency import run_in_threadpool
    router = APIRouter()

    async def resolve(date, scope):
        if not report:
            raise HTTPException(status_code=503, detail="資料庫無法使用")
        if scope not in ("all", "aggr"):
            raise HTTPException(status_code=400, detail="scope 只能是 all 或 aggr")
        if not date:
            ds = await run_in_threadpool(report.dbq.get_available_dates)
            if not ds:
                raise HTTPException(status_code=404, detail="尚無資料")
            date = ds[0]
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            raise HTTPException(status_code=400, detail="日期格式 YYYY-MM-DD")
        return date

    async def within(request, date):
        """沒登入只能產生最近 MAX_DAYS 個資料日（避免任意日期都去呼叫 AI）"""
        recent = (await run_in_threadpool(report.dbq.get_available_dates))[:MAX_DAYS]
        if date not in recent and not await check_authentication(request):
            raise HTTPException(status_code=403, detail=f"只提供最近 {MAX_DAYS} 個資料日的報告")

    @router.get("/report/etf.pdf")
    async def report_pdf(request: Request, date: str = Query(None), scope: str = Query("aggr"), refresh: bool = Query(False)):
        """主動式 ETF 日報 PDF；refresh=1 強制重寫（需登入）"""
        date = await resolve(date, scope)
        if refresh and not await check_authentication(request):
            raise HTTPException(status_code=401, detail="重新產生需要登入")
        await within(request, date)
        pdf = await run_in_threadpool(report.pdf, date, scope, templates, refresh)
        fn = f"etf_report_{scope}_{date.replace('-', '')}.pdf"
        return Response(pdf, media_type="application/pdf", headers={"Content-Disposition": f'inline; filename="{fn}"'})

    @router.get("/api/report/etf")
    async def report_json(request: Request, date: str = Query(None), scope: str = Query("aggr")):
        """日報說明與事實清單（JSON），給 BookReview 等外部程式用"""
        date = await resolve(date, scope)
        await within(request, date)
        rep = await run_in_threadpool(report.get, date, scope)
        return JSONResponse({"date": date, "scope": scope, "model": rep["model"], "created": rep["created"], "notes": rep["notes"],
                             "ai": rep["ai"], "facts": {k: v for k, v in rep["facts"].items() if not k.startswith("_")}})

    return router
