import requests, json
H={"User-Agent":"Mozilla/5.0"}
r=requests.post("https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes",data={"date":"2026/10/01","id":"","response":"json"},headers=H,timeout=60)
j=r.json(); t=j["tables"][0]
out=[str(j.get("date")), json.dumps(t.get("fields"),ensure_ascii=False)]+[json.dumps(x,ensure_ascii=False) for x in t["data"][:3]]
for code in ("3293","6147","5347","3105","8069"):
    out+= [json.dumps(x,ensure_ascii=False) for x in t["data"] if x[0]==code]
open("debug/tpex_result.txt","w").write("\n".join(out)); print("\n".join(out))
