"""三大法人同買「今日觀察」：事實清單 → Gemini 撰寫 → 數字核對 → 網頁區塊與 PDF

與 etf_report 共用：外部資料（Market）、Gemini 呼叫、數字核對、資料表 etf_report（scope='inst'）
"""
import datetime as dt
import logging
import re

import etf_report as ER

logger = logging.getLogger(__name__)
SCOPE = "inst"
LIST_MAX = 45        # PDF 三大同買表最多列幾檔


def ymd(date):
    return date.replace("-", "")


def dash(date):
    d = ymd(date)
    return f"{d[:4]}-{d[4:6]}-{d[6:]}"


class InstReport(ER.Report):
    PROMPT = "inst_observe_prompt.txt"
    TAG = "inst-report"

    def __init__(self, base):
        # 共用 etf_report 的資料庫、外部資料快取；不再建表
        self.db, self.dbq, self.in_scope, self.inst, self.market = base.db, base.dbq, base.in_scope, base.inst, base.market
        self.locks, self._pdf, self.last = {}, {}, {}

    def fingerprint(self, date, scope=SCOPE):
        r = self.inst._run(f"SELECT COUNT(*), COALESCE(SUM(trust_net), 0) FROM inst_daily WHERE trade_date = {self.inst._ph()}",
                           (ymd(date),), fetch=True) or [(0, 0)]
        try:
            etf = ER.Report.fingerprint(self, dash(date), "all")
        except Exception:
            etf = ""
        return f"{r[0][0]}-{r[0][1]}-{etf}"

    def build_facts(self, date, scope=SCOPE):
        D = dash(date)
        res = self.inst.compute(ymd(date))
        if not res:
            raise ValueError(f"查無 {D} 三大法人資料")
        rows, nd = res["rows"], res["n_days"]
        meta = self.market.meta()
        mk = self.market.day(D)
        price = mk.get("price", {})
        ind = lambda c: (meta.get(c) or {}).get("industry") or ""

        # 主動 ETF 當日淨增減（不分類範圍）
        etf_net = {}
        try:
            for m in self.in_scope("all", self._moves, D):
                etf_net[m["stock_code"]] = etf_net.get(m["stock_code"], 0) + (m["new_shares"] or 0) - (m["old_shares"] or 0)
        except Exception as e:
            logger.warning(f"[inst-report] 讀主動 ETF 異動失敗：{e}")
        ranked = sorted(etf_net.items(), key=lambda kv: kv[1])
        etf_buy = {c: ER.zh(v) for c, v in reversed(ranked) if v > 0}
        etf_buy = dict(list(etf_buy.items())[:15])
        etf_sell = {c: ER.zh(v) for c, v in ranked if v < 0}
        etf_sell = dict(list(etf_sell.items())[:15])

        def card(r, keys):
            o = {"代號": r["id"], "簡稱": r["name"], "產業別": ind(r["id"]), "股價漲跌幅%": price.get(r["id"])}
            for k, z in keys:
                o[z] = r.get(k)
            if r["id"] in etf_buy: o["主動ETF當日淨加碼張"] = etf_buy[r["id"]]
            if r["id"] in etf_sell: o["主動ETF當日淨減碼張"] = etf_sell[r["id"]]
            return o

        K3 = [("total", "三大合計張"), ("foreign", "外資張"), ("trust", "投信張"), ("dealer", "自營張"), ("co_streak", "同買連續天數"),
              ("co_days", f"{nd}日內同買天數"), ("foreign_streak", "外資連買天數"), ("trust_streak", "投信連買天數")]
        KFT = [("ft_total", "外資加投信張"), ("foreign", "外資張"), ("trust", "投信張"), ("dealer", "自營張"), ("ft_streak", "外資投信同買連續天數")]
        size = lambda r: min(abs(r["trust"] or 0), abs(r["foreign"] or 0))
        co = sorted([r for r in rows if r["cobuy"]], key=lambda r: -r["total"])
        ft = sorted([r for r in rows if r.get("ft_cobuy")], key=lambda r: -r["ft_total"])
        ft_only = [r for r in ft if not r["cobuy"]]          # 外資投信同買但自營沒一起買（三大同買另列）
        dt_ = sorted([r for r in rows if r.get("duel") == 1], key=lambda r: -size(r))
        df_ = sorted([r for r in rows if r.get("duel") == -1], key=lambda r: -size(r))
        sec = {}
        for r in co:
            s = sec.setdefault(ind(r["id"]) or "其他", {"檔數": 0, "三大合計張": 0, "股票": []})
            s["檔數"] += 1; s["三大合計張"] += r["total"]; s["股票"].append(r["name"])
        sec = dict(sorted(sec.items(), key=lambda kv: -kv[1]["三大合計張"]))
        idx, br, inst_yi = mk.get("index") or {}, mk.get("breadth") or {}, mk.get("inst_yi")
        w = res["window"]
        return {
            "資料日": D, "統計區間": f"{dash(w[0])}～{dash(w[1])}（{nd} 個交易日）", "_n_days": nd,
            "大盤": {"加權指數收盤": idx.get("close"), "漲跌點": idx.get("chg"), "漲跌幅%": idx.get("pct"),
                   "上漲家數": br.get("上漲(漲停)"), "下跌家數": br.get("下跌(跌停)")} if idx else None,
            "三大法人買賣超億元（上市）": inst_yi,
            "檔數統計": {"三大同買": len(co), "連續同買2天以上": sum(1 for r in co if r["co_streak"] >= 2), "外資投信同買": len(ft),
                     "土洋對作合計": len(dt_) + len(df_), "土洋對作_投信買外資賣": len(dt_), "土洋對作_外資買投信賣": len(df_),
                     "投信連買5天以上": sum(1 for r in rows if r["trust_streak"] >= 5),
                     "外資連買5天以上": sum(1 for r in rows if r["foreign_streak"] >= 5)},
            "三大同買前15": [card(r, K3) for r in co[:15]],
            "_三大同買清單": [card(r, K3) for r in co[:LIST_MAX]],      # PDF 表格用（底線開頭不送 AI）
            "_外資投信同買清單": [card(r, KFT) for r in ft_only[:LIST_MAX]],
            "_外資投信同買檔數": len(ft_only),
            "連續同買2天以上": [card(r, K3) for r in sorted([r for r in co if r["co_streak"] >= 2], key=lambda r: (-r["co_streak"], -r["total"]))[:10]],
            "三大同買族群彙總": sec,
            "三大同買股價表現": {"有股價的檔數": sum(1 for r in co if price.get(r["id"]) is not None),
                         "上漲檔數": sum(1 for r in co if (price.get(r["id"]) or 0) > 0),
                         "下跌檔數": sum(1 for r in co if (price.get(r["id"]) or 0) < 0)},
            "外資投信同買前10（不含三大同買）": [card(r, KFT) for r in ft_only[:10]],
            "土洋對作_投信買外資賣前8": [card(r, [("trust", "投信張"), ("foreign", "外資張"), ("duel_streak", "對作連續天數"), ("trust_streak", "投信連買天數")]) for r in dt_[:8]],
            "土洋對作_外資買投信賣前8": [card(r, [("foreign", "外資張"), ("trust", "投信張"), ("duel_streak", "對作連續天數"), ("foreign_streak", "外資連買天數")]) for r in df_[:8]],
            "投信連買最久前8": [card(r, [("trust_streak", "投信連買天數"), ("trust", "投信當日張"), ("trust_sum", f"投信{nd}日累計張")])
                          for r in sorted(rows, key=lambda r: (-r["trust_streak"], -r["trust_sum"]))[:8]],
            "外資連買最久前8": [card(r, [("foreign_streak", "外資連買天數"), ("foreign", "外資當日張"), ("foreign_sum", f"外資{nd}日累計張")])
                          for r in sorted(rows, key=lambda r: (-r["foreign_streak"], -r["foreign_sum"]))[:8]],
            "說明": "張＝股數/1000 四捨五入；外資含外資自營商；自營商＝自行買賣＋避險；主動ETF欄位只在該股進入主動ETF當日淨加碼或淨減碼前15名時出現",
        }

    def fallback(self, f):
        k, i, m = f["檔數統計"], f.get("三大法人買賣超億元（上市）"), f.get("大盤")
        co = f["三大同買前15"]
        sec = list(f["三大同買族群彙總"].items())
        b = []
        if co: b.append({"type": "up", "text": f"三大同買 {k['三大同買']} 檔，{co[0]['簡稱']}三大合計 {co[0]['三大合計張']:,} 張最多"})
        if sec: b.append({"type": "up", "text": f"同買以{sec[0][0]}最多，{sec[0][1]['檔數']} 檔合計 {sec[0][1]['三大合計張']:,} 張"})
        b.append({"type": "warn", "text": f"土洋對作 {k['土洋對作合計']} 檔，投信買外資賣 {k['土洋對作_投信買外資賣']} 檔"})
        t = f["投信連買最久前8"]
        if t: b.append({"type": "core", "text": f"{t[0]['簡稱']}投信連買 {t[0]['投信連買天數']} 天最久"})
        flow = ""
        if m and m.get("加權指數收盤"):
            flow += f"加權指數收 {m['加權指數收盤']:,.2f} 點，漲跌 {m['漲跌點']:+,.2f} 點。"
        if i:
            flow += f"上市三大法人合計買賣超 {i['合計']:+,.2f} 億元，外資 {i['外資及陸資']:+,.2f} 億元、投信 {i['投信']:+,.2f} 億元。"
        wl = []
        for r in co[:3]:
            wl.append({"code": r["代號"], "tag": "三大同買", "text": f"三大合計 {r['三大合計張']:,} 張，同買連續 {r['同買連續天數']} 天。"})
        for r in f["土洋對作_投信買外資賣前8"][:2]:
            wl.append({"code": r["代號"], "tag": "土洋對作", "text": f"投信買 {r['投信張']:,} 張、外資賣 {abs(r['外資張']):,} 張。"})
        return {
            "headline": f"三大同買 {k['三大同買']} 檔，土洋對作 {k['土洋對作合計']} 檔",
            "bullets": b, "flow": flow or "（無大盤資料）",
            "cobuy": "三大同買族群：" + "、".join(f"{n} {s['檔數']} 檔" for n, s in sec[:5]) + "。",
            "duel": "投信買、外資賣：" + "、".join(r["簡稱"] for r in f["土洋對作_投信買外資賣前8"][:5]) +
                    "；外資買、投信賣：" + "、".join(r["簡稱"] for r in f["土洋對作_外資買投信賣前8"][:5]) + "。",
            "streak": "投信連買最久：" + "、".join(f"{r['簡稱']} {r['投信連買天數']} 天" for r in t[:3]) + "。",
            "etf_link": "", "watch": wl,
            "risk": "單日訊號僅供參考，請搭配個股基本面與股價位置自行判斷。",
        }

    def render(self, rep, templates):
        env = templates.env
        env.filters.setdefault("n", lambda v: "—" if v is None else f"{v:,.0f}")
        env.filters.setdefault("pct", lambda v: "—" if v is None else f"{v:+.2f}%")
        f = rep["facts"]
        if "_外資投信同買清單" not in f:                # 舊版報告只存前 15／10 檔：只重算事實補清單，不重呼叫 AI
            try:
                new = self.build_facts(f["資料日"])
                f = {**f, **{k: new[k] for k in ("_三大同買清單", "_外資投信同買清單", "_外資投信同買檔數")}}
            except Exception as e:
                logger.warning(f"[{self.TAG}] 補三大同買清單失敗：{e}")
        stock = {}
        for v in f.values():
            if isinstance(v, list):
                for r in v:
                    if isinstance(r, dict) and "代號" in r:
                        stock.setdefault(r["代號"], r)
        ai = dict(rep["ai"])
        ai["watch"] = [w for w in ai.get("watch", []) if w.get("code") in stock]
        return env.get_template("inst_report_pdf.html").render(
            f=f, ai=ai, stock=stock, dd=f["資料日"], nd=f.get("_n_days", 40), created=rep["created"], model=rep["model"])

    def run_daily(self):
        ds = self.inst.dates_in_db()
        if not ds:
            return "沒有三大法人資料"
        r = self.get(dash(ds[-1]), SCOPE)
        return f"{dash(ds[-1])} {r['model']}"


report = None


def init(base):
    global report
    if base and base.inst:
        report = InstReport(base)
    return report


def create_router(templates, check_authentication):
    from fastapi import APIRouter, HTTPException, Query, Request
    from fastapi.responses import JSONResponse, Response
    from starlette.concurrency import run_in_threadpool
    router = APIRouter()

    async def resolve(date):
        if not report:
            raise HTTPException(status_code=503, detail="資料庫無法使用")
        if not date:
            ds = await run_in_threadpool(report.inst.dates_in_db)
            if not ds:
                raise HTTPException(status_code=404, detail="尚無資料")
            date = ds[-1]
        if not re.fullmatch(r"\d{4}-?\d{2}-?\d{2}", date):
            raise HTTPException(status_code=400, detail="日期格式 YYYYMMDD 或 YYYY-MM-DD")
        return dash(date)

    @router.get("/report/inst.pdf")
    async def inst_pdf(request: Request, date: str = Query(None), refresh: bool = Query(False)):
        """三大法人今日觀察 PDF；refresh=1 強制重寫（需登入）"""
        d = await resolve(date)
        if refresh and not await check_authentication(request):
            raise HTTPException(status_code=401, detail="重新產生需要登入")
        recent = [dash(x) for x in (await run_in_threadpool(report.inst.dates_in_db))[-ER.MAX_DAYS:]]
        if d not in recent and not await check_authentication(request):
            raise HTTPException(status_code=403, detail=f"只提供最近 {ER.MAX_DAYS} 個交易日的報告")
        try:
            pdf = await run_in_threadpool(report.pdf, d, SCOPE, templates, refresh)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        return Response(pdf, media_type="application/pdf",
                        headers={"Content-Disposition": f'inline; filename="inst_report_{ymd(d)}.pdf"'})

    @router.get("/api/inst-cobuy/observe")
    async def inst_observe(date: str = Query(None)):
        """網頁「今日觀察」：只讀已產生的版本，不在這裡呼叫 AI（避免網頁等太久）"""
        d = await resolve(date)
        rep = await run_in_threadpool(report.stored, d, SCOPE)
        if not rep:
            raise HTTPException(status_code=404, detail="今日觀察尚未產生")
        return JSONResponse({"date": ymd(d), "model": rep["model"], "created": rep["created"], "ai": rep["ai"]})

    return router
