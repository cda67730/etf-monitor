"""AI 籌碼焦點 10 檔：每天晚上把網站上的多元資料（權證除外）合起來，程式先算分，再交給 Gemini 排出 10 檔

流程（與 etf_report／inst_report 共用 Gemini 呼叫、查證、數字核對與資料表 etf_report，scope='pick'）：
1. 程式算「籌碼分數」：三大法人（同買、外資投信同買、連買、近 5 日累計、土洋對作）、
   主動 ETF（當日加碼／新進／減碼檔數、近 5 個資料日加碼次數）、量化研究（慢牛、回測突破、Autoencoder）
   多來源重疊（法人／主動 ETF／量化研究同時偏多）加分，且一律納入候選；跨 ETF 共同持有檔數也計分
2. 取分數前 CANDIDATES 檔（＋重疊股）連同大盤背景（市場情緒、微台散戶）交給 Gemini，從中排出 10 檔、各寫一句理由
3. 查證（開 Google 搜尋）→ 程式核對數字 → 只留候選名單內的代號，不足 10 檔依分數補上
4. 排程平日 21:30（SCHED_PICK）；資料沒變不重寫；首頁下方只讀已產生的版本
"""
import logging
import re

import etf_report as ER
from inst_report import dash, ymd

logger = logging.getLogger(__name__)
SCOPE = "pick"
TOP = 10            # 最後名單檔數
CANDIDATES = 30     # 交給 AI 挑選的候選檔數
OVERLAP_MAX = 15    # 分數不在前 30 但多來源重疊的，最多再補幾檔進候選
LAB_MAX_AGE = 7     # 量化研究結果超過幾天就不採用
CODE_KEYS = ("code", "stock_id", "stock_code", "id", "symbol", "代號", "股票代號", "ticker")


def _lab_code(row):
    """從本機推上來的一列找出股票代號（欄位名稱不固定）"""
    for k in CODE_KEYS:
        v = row.get(k)
        if v is not None and re.fullmatch(r"\d{4}[A-Z]?", str(v).strip().split(".")[0]):
            return str(v).strip().split(".")[0]
    for k, v in row.items():
        if not str(k).startswith("_") and isinstance(v, str) and re.fullmatch(r"\d{4}", v.strip()):
            return v.strip()
    return None


class PickReport(ER.Report):
    PROMPT = "pick_prompt.txt"
    TAG = "pick"

    def __init__(self, base, inst_report=None):
        self.db, self.dbq, self.in_scope, self.inst, self.market = base.db, base.dbq, base.in_scope, base.inst, base.market
        self.inst_report = inst_report
        self.locks, self._pdf, self.last = {}, {}, {}

    # ---- 外部模組（主程式啟動後才有，用到時再取）
    @staticmethod
    def _lab():
        try:
            import lab_results
            return lab_results.store
        except Exception:
            return None

    @staticmethod
    def _mood():
        try:
            import market_mood
            return market_mood.store.report() if market_mood.store else None
        except Exception as e:
            logger.warning(f"[pick] 讀市場情緒失敗：{e}")
            return None

    def _held(self, d):
        dbq = self.dbq
        return dbq.execute_query(f"SELECT stock_code, COUNT(DISTINCT etf_code) AS n FROM etf_holdings WHERE update_date = "
                                 f"{dbq._get_placeholder()} AND {dbq._scope_sql('etf_code')} GROUP BY stock_code", (d,), fetch="all") or []

    def _etf_dates(self, D):
        return [d for d in self.in_scope("all", self.dbq.get_available_dates) if d <= D]

    def _lab_hits(self, D):
        """{代號: [(kind, _flag)]}；只採用 D 往前 LAB_MAX_AGE 天內的最新一份"""
        st, out, used = self._lab(), {}, {}
        if not st:
            return out, used
        import datetime as dt
        lo = (dt.date.fromisoformat(D) - dt.timedelta(days=LAB_MAX_AGE)).isoformat()
        for kind in ("slow_bull", "backtest", "ae_monitor"):
            try:
                ds = [x["date"] for x in st.dates(kind, 20) if lo <= x["date"] <= D]
                if not ds:
                    continue
                rep = st.get(kind, ds[0])
                used[kind] = rep["date"]
                for r in rep.get("rows") or []:
                    c = _lab_code(r)
                    if c:
                        out.setdefault(c, []).append((kind, r.get("_flag")))
            except Exception as e:
                logger.warning(f"[pick] 讀量化研究 {kind} 失敗：{e}")
        return out, used

    def fingerprint(self, date, scope=SCOPE):
        D = dash(date)
        parts = [self.inst_report.fingerprint(D) if self.inst_report else ""]
        st = self._lab()
        if st:
            for kind in ("slow_bull", "backtest", "ae_monitor"):
                try:
                    x = st.dates(kind, 1)
                    parts.append(f"{x[0]['date']}@{x[0]['received']}" if x else "-")
                except Exception:
                    parts.append("?")
        return "|".join(parts)

    # ---- 算分
    def build_facts(self, date, scope=SCOPE):
        D = dash(date)
        res = self.inst.compute(ymd(D))
        if not res:
            raise ValueError(f"查無 {D} 三大法人資料")
        rows = {r["id"]: r for r in res["rows"]}
        meta = self.market.meta()
        mk = self.market.day(D)
        price = mk.get("price", {})

        # 主動 ETF：最近一個 ≤ D 的資料日當天的異動，與近 5 個資料日的加碼次數
        etf_today, etf5, etf_d = {}, {}, None
        try:
            eds = self._etf_dates(D)[:5]
            etf_d = eds[0] if eds else None
            for i, d in enumerate(eds):
                for m in self.in_scope("all", self._moves, d):
                    c, t = m["stock_code"], m["change_type"]
                    if i == 0:
                        e = etf_today.setdefault(c, {"add": 0, "new": 0, "cut": 0, "net": 0, "name": m["stock_name"]})
                        e["add"] += t == "INCREASED"; e["new"] += t == "NEW"; e["cut"] += t in ("DECREASED", "REMOVED")
                        e["net"] += (m["new_shares"] or 0) - (m["old_shares"] or 0)
                    if t in ("INCREASED", "NEW"):
                        etf5[c] = etf5.get(c, 0) + 1
        except Exception as e:
            logger.warning(f"[pick] 讀主動 ETF 異動失敗：{e}")
        held = {}                                                 # 跨 ETF 重複持股：當日幾檔主動 ETF 同時持有
        if etf_d:
            try:
                held = {r["stock_code"]: r["n"] for r in self.in_scope("all", self._held, etf_d)}
            except Exception as e:
                logger.warning(f"[pick] 讀跨 ETF 持股失敗：{e}")

        lab, lab_used = self._lab_hits(D)

        # 近 5 日外資＋投信淨買超的百分位（只在有買超的股票之間比）
        ft5 = sorted(v for v in ((r["f5"] or 0) + (r["t5"] or 0) for r in rows.values()) if v > 0)
        pct = lambda v: (sum(1 for x in ft5 if x <= v) / len(ft5)) if v > 0 and ft5 else 0

        codes = set(rows) | set(etf_today) | set(lab)
        overlap = {}                                              # 代號 -> 同時出現訊號的來源
        cands = []
        for c in codes:
            if not re.fullmatch(r"\d{4}", c):                     # 只看 4 碼普通股
                continue
            r, e, lb = rows.get(c) or {}, etf_today.get(c) or {}, lab.get(c) or []
            s, sig = 0.0, []
            if r.get("cobuy"):
                s += 3 + min(r["co_streak"], 5); sig.append(f"三大同買連{r['co_streak']}天" if r["co_streak"] > 1 else "三大同買")
            elif r.get("ft_cobuy"):
                s += 2 + 0.5 * min(r["ft_streak"], 5); sig.append(f"外資投信同買連{r['ft_streak']}天" if r["ft_streak"] > 1 else "外資投信同買")
            if r.get("trust_streak", 0) >= 3:
                s += 0.3 * min(r["trust_streak"], 10); sig.append(f"投信連買{r['trust_streak']}天")
            if r.get("foreign_streak", 0) >= 3:
                s += 0.2 * min(r["foreign_streak"], 10); sig.append(f"外資連買{r['foreign_streak']}天")
            s += 2 * pct((r.get("f5") or 0) + (r.get("t5") or 0))
            if r.get("duel") == 1:
                s += 0.5; sig.append("投信買外資賣")
            elif r.get("duel") == -1:
                s -= 0.5
            if e:
                s += 1.5 * min(e["add"], 6) + 1.0 * min(e["new"], 4) - 1.0 * min(e["cut"], 4)
                if e["add"]: sig.append(f"主動ETF {e['add']}檔加碼")
                if e["new"]: sig.append(f"主動ETF {e['new']}檔新進")
                if e["cut"]: sig.append(f"主動ETF {e['cut']}檔減碼")
            s += 0.3 * min(etf5.get(c, 0), 15)
            nh = held.get(c, 0)
            if nh >= 3:
                s += 0.4 * min(nh, 10); sig.append(f"{nh}檔主動ETF共同持有")
            for kind, flag in lb:
                if kind == "slow_bull":
                    if flag == "stopped": s -= 3; sig.append("慢牛已停損")
                    elif flag == "market_block": sig.append("慢牛（大盤不買）")
                    elif flag == "new": s += 3; sig.append("慢牛新訊號")
                    else: s += 2; sig.append("慢牛持有中")
                elif kind == "backtest":
                    s += 2; sig.append("回測突破")
                elif kind == "ae_monitor":
                    s += 1; sig.append("Autoencoder 監測")
            # 多來源重疊：法人、主動 ETF、量化研究同時有偏多訊號的加分，並保證進候選
            src = []
            if r.get("cobuy") or r.get("ft_cobuy") or r.get("trust_streak", 0) >= 3 or r.get("foreign_streak", 0) >= 5:
                src.append("法人")
            if (e and (e["add"] or e["new"])) or etf5.get(c, 0) >= 2 or nh >= 5:
                src.append("主動ETF")
            if any(k != "slow_bull" or f not in ("stopped", "market_block") for k, f in lb):
                src.append("量化研究")
            if len(src) >= 2:
                s += 2 if len(src) == 2 else 4
                sig.insert(0, "＋".join(src) + "重疊")
                overlap[c] = src
            # 流動性：外資＋投信 5 日合計太小、又沒有 ETF／量化訊號的不列入
            if s <= 0 or (abs((r.get("f5") or 0) + (r.get("t5") or 0)) < 300 and not e and not lb):
                continue
            nm = r.get("name") or (meta.get(c) or {}).get("short") or e.get("name") or c
            cands.append({
                "代號": c, "簡稱": nm, "產業別": (meta.get(c) or {}).get("industry") or "",
                "籌碼分數": round(s, 1), "訊號": sig, "股價漲跌幅%": price.get(c),
                "外資張": r.get("foreign"), "投信張": r.get("trust"), "自營張": r.get("dealer"),
                "近5日外資張": r.get("f5"), "近5日投信張": r.get("t5"),
                "外資連買天數": r.get("foreign_streak"), "投信連買天數": r.get("trust_streak"),
                "主動ETF當日淨增減張": ER.zh(e["net"]) if e else None,
                "近5個資料日主動ETF加碼次數": etf5.get(c) or None,
                "主動ETF共同持有檔數": nh or None,
                "訊號重疊來源": overlap.get(c),
            })
        cands.sort(key=lambda x: -x["籌碼分數"])
        top = cands[:CANDIDATES]                                  # 多來源重疊的一律納入候選（最多再加 OVERLAP_MAX 檔）
        top += [x for x in cands[CANDIDATES:] if x["代號"] in overlap][:OVERLAP_MAX]
        cands = top
        for x in cands:                                           # 沒值的欄位不送，減少雜訊
            for k in [k for k, v in x.items() if v is None]:
                del x[k]

        bg = {}
        mood = self._mood()
        if mood:
            it = {x["id"]: x for x in mood.get("items") or []}
            for k, z in (("vix", "VIX"), ("tw_vix", "台股VIX"), ("cnn_fg", "CNN恐懼貪婪"), ("tmf_retail", "微台散戶淨多空口數")):
                x = it.get(k)
                if x and x.get("signal") != "gray":
                    bg[z] = f"{x.get('display')}（{x.get('label')}）"
            bg["賣出條件成立"] = f"{mood.get('n_met')}/{len(mood.get('sells') or [])}"
        idx = mk.get("index") or {}
        w = res["window"]
        return {
            "資料日": D, "三大法人統計區間": f"{dash(w[0])}～{dash(w[1])}（{res['n_days']} 個交易日）",
            "主動ETF資料日": etf_d, "量化研究資料日": {"慢牛": lab_used.get("slow_bull"), "回測突破": lab_used.get("backtest"),
                                                "Autoencoder": lab_used.get("ae_monitor")},
            "大盤": {"加權指數收盤": idx.get("close"), "漲跌點": idx.get("chg"), "漲跌幅%": idx.get("pct")} if idx else None,
            "市場情緒": bg or None,
            "候選名單": cands,
            "重疊檔數": {"兩種來源": sum(1 for v in overlap.values() if len(v) == 2), "三種來源": sum(1 for v in overlap.values() if len(v) >= 3)},
            "說明": "訊號重疊來源＝法人、主動ETF、量化研究中同時偏多的來源；籌碼分數由程式依三大法人、主動ETF、量化研究訊號加總（越高代表籌碼面越一致偏多），不含技術面與基本面；張＝股數/1000",
        }

    # ---- 文字
    def fallback(self, f):
        out = []
        for x in f["候選名單"]:
            sig = "、".join(x["訊號"][:3]) or "籌碼分數居前"
            out.append({"code": x["代號"], "tag": (x["訊號"] or ["籌碼偏多"])[0][:8], "text": f"{x['簡稱']}：{sig}。"})
        n = len(f["候選名單"])
        return {"summary": f"依三大法人、主動 ETF 與量化研究訊號加總，從 {n} 檔候選中列出籌碼面最一致的 {min(n, TOP)} 檔。",
                "picks": out}

    def write(self, facts):
        ai, model, notes = super().write(facts)
        cand = {x["代號"] for x in facts["候選名單"]}
        tpl = {x["code"]: x for x in self.fallback(facts)["picks"]}
        seen, out = set(), []
        for it in ai.get("picks") or []:                          # 只留候選名單內的代號，去重
            c = str(it.get("code", "")).strip() if isinstance(it, dict) else ""
            if c in cand and c not in seen and it.get("text"):
                seen.add(c); out.append({"code": c, "tag": str(it.get("tag") or tpl[c]["tag"])[:10], "text": it["text"]})
            if len(out) >= TOP:
                break
        for x in facts["候選名單"]:                                # 不足 10 檔依分數補
            if len(out) >= TOP:
                break
            if x["代號"] not in seen:
                seen.add(x["代號"]); out.append(tpl[x["代號"]])
        ai["picks"] = out
        return ai, model, notes

    def run_daily(self):
        ds = self.inst.dates_in_db()
        if not ds:
            return "沒有三大法人資料"
        r = self.get(dash(ds[-1]), SCOPE)
        return f"{dash(ds[-1])} {r['model']}"

    def latest(self):
        """首頁用：最新一份已產生的名單（不呼叫 AI），附上每檔的簡稱與訊號"""
        row = self._q("SELECT d FROM etf_report WHERE scope = ? ORDER BY d DESC LIMIT 1", (SCOPE,), fetch="one")
        if not row:
            return None
        rep = self.stored(row["d"], SCOPE)
        C = {x["代號"]: x for x in rep["facts"].get("候選名單", [])}
        picks = []
        for i, p in enumerate(rep["ai"].get("picks") or [], 1):
            x = C.get(p["code"], {})
            picks.append({"rank": i, "code": p["code"], "name": x.get("簡稱", ""), "tag": p.get("tag", ""), "text": p["text"],
                          "score": x.get("籌碼分數"), "signals": x.get("訊號", []), "pct": x.get("股價漲跌幅%")})
        return {"date": row["d"], "summary": rep["ai"].get("summary", ""), "picks": picks,
                "model": rep["model"], "created": rep["created"]}


report = None


def init(base, inst_report=None):
    global report
    if base and base.inst:
        report = PickReport(base, inst_report)
    return report


def create_router(check_authentication):
    from fastapi import APIRouter, HTTPException, Query, Request
    from fastapi.responses import JSONResponse
    from starlette.concurrency import run_in_threadpool
    router = APIRouter()

    @router.get("/api/picks")
    async def picks(request: Request, date: str = Query(None), refresh: bool = Query(False)):
        """AI 籌碼焦點 10 檔：預設只讀已產生的版本；refresh=1 立即產生（需登入）"""
        if not report:
            raise HTTPException(status_code=503, detail="資料庫無法使用")
        if refresh:
            if not await check_authentication(request):
                raise HTTPException(status_code=401, detail="重新產生需要登入")
            ds = await run_in_threadpool(report.inst.dates_in_db)
            d = dash(date) if date else (dash(ds[-1]) if ds else None)
            if not d:
                raise HTTPException(status_code=404, detail="尚無資料")
            try:
                await run_in_threadpool(report.get, d, SCOPE, True)
            except ValueError as e:
                raise HTTPException(status_code=404, detail=str(e))
        if date and not refresh:
            rep = await run_in_threadpool(report.stored, dash(date), SCOPE)
            if not rep:
                raise HTTPException(status_code=404, detail="這一天沒有名單")
            return JSONResponse({"date": dash(date), "model": rep["model"], "created": rep["created"], "notes": rep["notes"],
                                 "ai": rep["ai"], "facts": rep["facts"]})
        r = await run_in_threadpool(report.latest)
        if not r:
            raise HTTPException(status_code=404, detail="名單尚未產生")
        return JSONResponse(r)

    return router
