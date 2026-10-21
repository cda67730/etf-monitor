import requests, json, time
H={"User-Agent":"Mozilla/5.0"}
out=[]
def t(name, fn):
    t0=time.time()
    try:
        r=fn(); out.append(f"## {name}: {r.status_code} {len(r.content)}B {time.time()-t0:.1f}s ct={r.headers.get('content-type')}\n{r.text[:600]}\n")
    except Exception as e: out.append(f"## {name}: ERR {e}\n")
t("twse openapi IS", lambda: requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap06_L_ci",headers=H,timeout=30))
t("twse openapi BS", lambda: requests.get("https://openapi.twse.com.tw/v1/opendata/t187ap07_L_ci",headers=H,timeout=30))
t("tpex openapi IS", lambda: requests.get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap06_O_ci",headers=H,timeout=30))
t("tpex openapi BS", lambda: requests.get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap07_O_ci",headers=H,timeout=30))
t("mops t163sb04 2025Q4 sii", lambda: requests.post("https://mops.twse.com.tw/mops/web/ajax_t163sb04",headers=H,timeout=30,data={"encodeURIComponent":1,"step":1,"firstin":1,"off":1,"isQuery":"Y","TYPEK":"sii","year":"114","season":"04"}))
t("mopsov t163sb04", lambda: requests.post("https://mopsov.twse.com.tw/mops/web/ajax_t163sb04",headers=H,timeout=30,data={"encodeURIComponent":1,"step":1,"firstin":1,"off":1,"isQuery":"Y","TYPEK":"sii","year":"114","season":"04"}))
t("finmind fs 2330", lambda: requests.get("https://api.finmindtrade.com/api/v4/data",params={"dataset":"TaiwanStockFinancialStatements","data_id":"2330","start_date":"2021-01-01"},timeout=30))
t("finmind fs all-stock one date", lambda: requests.get("https://api.finmindtrade.com/api/v4/data",params={"dataset":"TaiwanStockFinancialStatements","start_date":"2026-06-30","end_date":"2026-06-30"},timeout=60))
open("debug/fut_result.txt","w").write("\n".join(out))
