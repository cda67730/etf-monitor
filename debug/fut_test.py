import requests, re, json
H={"User-Agent":"Mozilla/5.0","Referer":"https://mis.taifex.com.tw/futures/VolatilityQuotes/"}
out=[]
B="https://mis.taifex.com.tw"
r=requests.get(B+"/futures/VolatilityQuotes/",headers=H,timeout=30)
js=[j for j in re.findall(r'src="([^"]+\.js)"',r.text) if "_nuxt" in j]
seen=set()
while js:
    j=js.pop(0)
    if j in seen: continue
    seen.add(j)
    t=requests.get(B+j,headers=H,timeout=30).text
    js += ["/futures/_nuxt/"+x for x in set(re.findall(r'([0-9a-f]{7}\.js)',t)) if "/futures/_nuxt/"+x not in seen][:40]
    hits=sorted(set(re.findall(r'["\'`]([^"\'`]*api/[^"\'`]{2,60})["\'`]',t)))
    vix=[t[max(0,m.start()-200):m.start()+200].replace("\n"," ") for m in re.finditer(r'(?i)volatil|VIX',t)][:3]
    if hits or vix: out.append(f"== {j} len={len(t)} apis={hits[:80]}"); out+=["   ctx: "+v for v in vix]
    if len(seen)>60: break
# try likely endpoints
for path,body in [("/futures/api/getQuoteListVIX",{}),("/futures/api/getVIX",{}),("/futures/api/getQuoteList",{"MarketType":"0","SymbolType":"F","KindID":"1","CID":"","ExpireMonth":"","RowSize":"全部","PageNo":"","SortColumn":"","AscDesc":"A"})]:
    try:
        rr=requests.post(B+path,json=body,headers=H,timeout=20); out.append(f"POST {path} {rr.status_code} {rr.text[:300]}")
    except Exception as e: out.append(f"POST {path} ERR {e}")
open("debug/fut_result.txt","w").write("\n".join(out))
