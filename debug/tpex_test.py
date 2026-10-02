import requests, json
H={"User-Agent":"Mozilla/5.0"}
urls={
 "new_dailyQuotes":"https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes?date=2026/10/01&id=&response=json",
 "new_dailyQ":"https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQ?date=2026%2F10%2F01&id=&response=json",
 "old_php":"https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&d=115/10/01&o=json",
 "openapi":"https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes",
}
out=[]
for k,u in urls.items():
    for m in ("get","post"):
        try:
            r=(requests.get(u,headers=H,timeout=40) if m=="get" else requests.post(u.split("?")[0],data={"date":"2026/10/01","id":"","response":"json"},headers=H,timeout=40))
            t=r.text
            out.append(f"== {k} {m} {r.status_code} {r.headers.get('content-type')} len={len(t)}\n{t[:700]}")
        except Exception as e: out.append(f"== {k} {m} ERR {e}")
        if "openapi" in k or "php" in k: break
open("debug/tpex_result.txt","w").write("\n".join(out)); print("\n".join(out))
