import requests, json
r=requests.post("https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes",data={"date":"2026/10/01","id":"","response":"json"},headers={"User-Agent":"Mozilla/5.0"},timeout=60)
t=r.json()["tables"][0]; out={}
for x in t["data"]:
    try:
        c=float(x[2].replace(",","")); d=float(x[3].strip().replace("+","").replace(",",""))
        out[x[0]]=round(d/(c-d)*100,2) if c-d else 0.0
    except Exception: pass
json.dump(out,open("debug/report/tpex_pct.json","w"),ensure_ascii=False); print(len(out))
