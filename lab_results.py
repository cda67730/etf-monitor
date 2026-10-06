"""量化研究結果（本機 breakout_detector 推上來的每日結果）：慢牛、回測突破、Autoencoder 監測

- 寫入：POST /api/ingest/{kind}，用 HMAC 簽章驗證（Railway 環境變數 INGEST_SECRET，本機存同一串）
    標頭 X-Timestamp：Unix 秒；X-Signature：hex(HMAC-SHA256(secret, f"{timestamp}.{原始 body}"))
    與伺服器時間差超過 5 分鐘、簽章不符、沒設 INGEST_SECRET 一律拒收；token 本身不在網路上傳
    body：{"date": "YYYY-MM-DD", "rows": [{...}, ...], "columns": [{"key", "label", "type"}]（可省略）, "note": "..."（可省略）}
    同一 kind、同一天重送就覆蓋
- 讀取：/lab 頁面與 /api/lab/*，都要登入網站密碼（跟 ETF 管理同一組）；ingest 的金鑰不能拿來讀
- 資料表 lab_result(kind, d, payload JSON, received)
"""
import datetime as dt
import hashlib
import hmac
import json
import logging
import os
import re
import time

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)
KINDS = {"slow_bull": "慢牛", "backtest": "回測突破", "ae_monitor": "Autoencoder 監測"}
MAX_BODY = 3 * 1024 * 1024        # 一次最多 3MB
MAX_ROWS = 5000
SKEW = 300                        # 時間戳容許誤差（秒）
TYPES = {"text", "code", "int", "float", "pct", "date", "signed"}


def secret():
    return os.getenv("INGEST_SECRET", "").strip()


def sign(key, ts, body: bytes):
    return hmac.new(key.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()


class LabStore:
    def __init__(self, db):
        self.db = db
        self._q("CREATE TABLE IF NOT EXISTS lab_result (kind TEXT NOT NULL, d TEXT NOT NULL, payload TEXT NOT NULL, "
                "received TEXT, PRIMARY KEY (kind, d))")

    def _q(self, sql, params=(), fetch="none"):
        ph = "%s" if self.db.db_type == "postgresql" else "?"
        return self.db.execute_query(sql.replace("?", ph), params, fetch)

    def save(self, kind, d, payload):
        now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S")
        self._q("INSERT INTO lab_result (kind, d, payload, received) VALUES (?, ?, ?, ?) "
                "ON CONFLICT (kind, d) DO UPDATE SET payload = EXCLUDED.payload, received = EXCLUDED.received",
                (kind, d, json.dumps(payload, ensure_ascii=False), now))
        return now

    def dates(self, kind, limit=120):
        rows = self._q("SELECT d, received FROM lab_result WHERE kind = ? ORDER BY d DESC LIMIT ?", (kind, int(limit)), fetch="all") or []
        return [{"date": r["d"], "received": r["received"]} for r in rows]

    def get(self, kind, d=None):
        if d:
            r = self._q("SELECT d, payload, received FROM lab_result WHERE kind = ? AND d = ?", (kind, d), fetch="one")
        else:
            r = self._q("SELECT d, payload, received FROM lab_result WHERE kind = ? ORDER BY d DESC LIMIT 1", (kind,), fetch="one")
        if not r:
            return None
        p = json.loads(r["payload"])
        return {"kind": kind, "name": KINDS[kind], "date": r["d"], "received": r["received"], **p}

    def summary(self):
        out = {}
        for k in KINDS:
            r = self._q("SELECT d, received FROM lab_result WHERE kind = ? ORDER BY d DESC LIMIT 1", (k,), fetch="one")
            out[k] = {"name": KINDS[k], "date": r["d"] if r else None, "received": r["received"] if r else None}
        return out


def validate(body):
    """檢查推上來的內容；回傳整理後的 payload（date, rows, columns, note）"""
    if not isinstance(body, dict):
        raise ValueError("body 必須是 JSON 物件")
    d = str(body.get("date", ""))
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
        raise ValueError("date 必須是 YYYY-MM-DD")
    dt.date.fromisoformat(d)
    rows = body.get("rows")
    if not isinstance(rows, list) or not all(isinstance(r, dict) for r in rows):
        raise ValueError("rows 必須是物件陣列")
    if len(rows) > MAX_ROWS:
        raise ValueError(f"rows 最多 {MAX_ROWS} 筆")
    cols = body.get("columns")
    if cols is None:                                   # 沒給欄位定義就依第一筆的鍵自動產生
        keys = []
        for r in rows:
            for k in r:
                if k not in keys:
                    keys.append(k)
        cols = [{"key": k, "label": k} for k in keys]
    if not isinstance(cols, list) or not all(isinstance(c, dict) and c.get("key") for c in cols):
        raise ValueError("columns 必須是 [{key, label, type}] 陣列")
    cols = [{"key": str(c["key"]), "label": str(c.get("label") or c["key"]),
             "type": c.get("type") if c.get("type") in TYPES else None} for c in cols][:40]
    note = str(body.get("note") or "")[:2000]
    return d, {"rows": rows, "columns": cols, "note": note}


store = None


def init(db_config):
    global store
    if not db_config:
        return None
    try:
        store = LabStore(db_config)
    except Exception as e:
        logger.error(f"❌ 量化研究資料表初始化失敗: {e}")
    return store


def create_router(templates, check_authentication):
    router = APIRouter()

    def need():
        if not store:
            raise HTTPException(status_code=503, detail="資料庫無法使用")
        return store

    @router.post("/api/ingest/{kind}")
    async def ingest(kind: str, request: Request):
        key = secret()
        if not key:
            raise HTTPException(status_code=503, detail="伺服器未設定 INGEST_SECRET")
        if kind not in KINDS:
            raise HTTPException(status_code=404, detail=f"kind 只能是 {', '.join(KINDS)}")
        ts, sig = request.headers.get("x-timestamp", ""), request.headers.get("x-signature", "")
        if not ts.isdigit() or abs(time.time() - int(ts)) > SKEW:
            raise HTTPException(status_code=401, detail="時間戳無效或超過 5 分鐘")
        body = await request.body()
        if len(body) > MAX_BODY:
            raise HTTPException(status_code=413, detail="內容太大")
        if not hmac.compare_digest(sign(key, ts, body), sig.lower()):
            logger.warning(f"[lab] {kind} 簽章不符，拒收")
            raise HTTPException(status_code=401, detail="簽章不符")
        try:
            d, payload = validate(json.loads(body))
        except (ValueError, json.JSONDecodeError) as e:
            raise HTTPException(status_code=422, detail=str(e))
        received = await run_in_threadpool(need().save, kind, d, payload)
        logger.info(f"[lab] 收到 {KINDS[kind]} {d}：{len(payload['rows'])} 筆")
        return {"ok": True, "kind": kind, "date": d, "rows": len(payload["rows"]), "received": received}

    @router.get("/lab", response_class=HTMLResponse)
    async def lab_page(request: Request):
        if not await check_authentication(request):
            return RedirectResponse(url="/login?next=/lab", status_code=302)
        summary = await run_in_threadpool(need().summary) if store else {}
        return templates.TemplateResponse("lab.html", {"request": request, "kinds": KINDS, "summary": summary})

    @router.get("/api/lab/{kind}/dates")
    async def lab_dates(kind: str, request: Request):
        if not await check_authentication(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        if kind not in KINDS:
            raise HTTPException(status_code=404)
        return JSONResponse(await run_in_threadpool(need().dates, kind))

    @router.get("/api/lab/{kind}")
    async def lab_get(kind: str, request: Request, date: str = Query(None)):
        if not await check_authentication(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        if kind not in KINDS:
            raise HTTPException(status_code=404)
        r = await run_in_threadpool(need().get, kind, date)
        if not r:
            raise HTTPException(status_code=404, detail="尚無資料")
        return JSONResponse(r)

    return router
