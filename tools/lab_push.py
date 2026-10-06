"""把 breakout_detector 的每日結果推到 Danny大叔的股市觀測筆記（量化研究頁）

放在本機 breakout_detector 專案裡使用，只需要 requests：
    from lab_push import push
    push("slow_bull", "2026-10-06", rows, columns=[...], note="...")

環境變數（放 .env，不要進 git）：
    INGEST_SECRET  與 Railway 上設定的同一串
    INGEST_URL     預設 https://etf-monitor-production.up.railway.app

kind：slow_bull（慢牛）、backtest（回測突破）、ae_monitor（Autoencoder 監測）
rows：每列一個 dict，例如 {"code": "2330", "name": "台積電", "score": 0.87, "ret_20d": 12.3}
columns（可省略，省略就用 rows 的鍵當欄名）：
    [{"key": "code", "label": "代號", "type": "code"}, {"key": "score", "label": "分數", "type": "float"}, ...]
    type 可用 text、code、date、int、float、pct（顯示成 +12.30%，紅漲綠跌）、signed（正負號加紅綠）
同一個 kind、同一天重送會覆蓋，失敗可以直接重送。
"""
import hashlib
import hmac
import json
import math
import os
import time

import requests


def _clean(v):
    """numpy／pandas 型別轉成一般 JSON 型別；NaN、inf 轉成 None"""
    if hasattr(v, "item"):
        v = v.item()
    if isinstance(v, float) and not math.isfinite(v):
        return None
    if hasattr(v, "isoformat"):
        return v.isoformat()[:10]
    return v


def push(kind, date, rows, columns=None, note="", url=None, secret=None, timeout=60):
    secret = secret or os.environ["INGEST_SECRET"]
    url = (url or os.getenv("INGEST_URL", "https://etf-monitor-production.up.railway.app")).rstrip("/")
    payload = {"date": str(date)[:10], "rows": [{k: _clean(v) for k, v in r.items()} for r in rows], "note": note}
    if columns:
        payload["columns"] = columns
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ts = str(int(time.time()))
    sig = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
    r = requests.post(f"{url}/api/ingest/{kind}", data=body, timeout=timeout,
                      headers={"Content-Type": "application/json", "X-Timestamp": ts, "X-Signature": sig})
    if r.status_code != 200:
        raise RuntimeError(f"推送失敗 {r.status_code}：{r.text[:300]}")
    return r.json()


if __name__ == "__main__":      # 測試：python lab_push.py
    print(push("slow_bull", time.strftime("%Y-%m-%d"),
               [{"code": "2330", "name": "台積電", "score": 0.91, "ret_20d": 8.4}],
               columns=[{"key": "code", "label": "代號", "type": "code"}, {"key": "name", "label": "名稱", "type": "text"},
                        {"key": "score", "label": "分數", "type": "float"}, {"key": "ret_20d", "label": "20日漲幅", "type": "pct"}],
               note="測試推送"))
