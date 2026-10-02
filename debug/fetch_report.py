"""暫時用：從正式站與證交所／櫃買抓日報 PDF 試做需要的資料"""
import json, os, re, time, requests
B = "https://etf-monitor-production.up.railway.app"
OUT = "debug/report"; os.makedirs(OUT, exist_ok=True)
S = requests.Session(); S.headers["User-Agent"] = "Mozilla/5.0"
log = []
def get(url, name, kind="json", **kw):
    for i in range(3):
        try:
            r = S.get(url, timeout=60, **kw); r.raise_for_status()
            if "isin" in url: r.encoding = "big5"
            data = r.json() if kind == "json" else r.text
            with open(f"{OUT}/{name}", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False) if kind == "json" else f.write(data)
            log.append(f"OK {name} {len(r.content)}"); return data
        except Exception as e:
            err = e; time.sleep(3)
    log.append(f"FAIL {name} {url} {err}"); return None

day = get(f"{B}/api/etf-day?scope=all", "etf_day.json")
D = day["date"]; YMD = D.replace("-", "")
get(f"{B}/decreased-holdings?scope=all&date={D}", "decreased.html", kind="text")
get("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", "isin_twse.html", kind="text")
get(f"{B}/api/etf-day?scope=aggr&date={D}", "etf_day_aggr.json")
get(f"{B}/api/etfs?scope=every", "etfs.json")
cross = get(f"{B}/api/cross-holdings?scope=all&date={D}", "cross.json")
get(f"{B}/api/first-buys?scope=all&date={D}", "first_buys.json")
html = get(f"{B}/etf?scope=all", "etf_all.html", kind="text")
codes = sorted(set(re.findall(r'data-stock="([^"]+)"', html or "")))
for r in sorted((cross or {}).get("rows", []), key=lambda r: (-r["etf_count"], -r["total_shares"]))[:40]:
    codes.append(r["stock_code"])
for c in sorted(set(codes)):
    get(f"{B}/api/stock-trend?scope=all&days=30&stock_code={c}", f"trend_{c}.json")
for e in day["rows"]:
    get(f"{B}/api/etf-holdings?etf_code={e['code']}&date={D}", f"hold_{e['code']}.json")
get(f"{B}/api/inst-cobuy?date={YMD}", "inst.json")
# 證交所／櫃買官方
T = "https://www.twse.com.tw/rwd/zh"
get(f"{T}/afterTrading/MI_INDEX?date={YMD}&type=IND&response=json", "twse_index.json")
get(f"{T}/afterTrading/MI_INDEX?date={YMD}&type=ALLBUT0999&response=json", "twse_all.json")
get(f"{T}/fund/BFI82U?type=day&dayDate={YMD}&response=json", "twse_bfi82u.json")
get(f"{T}/afterTrading/FMTQIK?date={YMD}&response=json", "twse_fmtqik.json")
roc = f"{int(YMD[:4]) - 1911}/{YMD[4:6]}/{YMD[6:]}"
get(f"https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQ?date={D.replace('-', '/')}&type=EW&response=json", "tpex_daily.json")
get("https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O", "tpex_company.json")
open(f"{OUT}/_log.txt", "w").write(f"date {D}\n" + "\n".join(log))
print("\n".join(log))
