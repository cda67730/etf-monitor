import requests, re, json
H={"User-Agent":"Mozilla/5.0","Referer":"https://mis.taifex.com.tw/futures/VolatilityQuotes/"}
out=[]
try:
    r=requests.get("https://mis.taifex.com.tw/futures/VolatilityQuotes/",headers=H,timeout=30)
    out.append(f"page {r.status_code} len={len(r.text)}"); out.append(r.text[:1500])
    js=re.findall(r'src="([^"]+\.js)"',r.text)
    out.append("js="+json.dumps(js))
    for j in js[:6]:
        u=j if j.startswith("http") else "https://mis.taifex.com.tw"+(j if j.startswith("/") else "/futures/VolatilityQuotes/"+j)
        t=requests.get(u,headers=H,timeout=30).text
        apis=sorted(set(re.findall(r'["\'](/?(?:futures/)?api/[A-Za-z0-9_/]+)["\']',t)))
        out.append(f"== {u} len={len(t)} apis={apis[:60]}")
        for k in ("VIX","Volatility","vix"):
            for m in re.finditer(k,t):
                out.append("   ctx: "+t[max(0,m.start()-150):m.start()+150].replace("\n"," "))
                break
except Exception as e: out.append("ERR "+repr(e))
open("debug/fut_result.txt","w").write("\n".join(out))
