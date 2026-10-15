import requests, json, time
B="https://etf-monitor-production.up.railway.app"
out=[]
for u in ["/api/report/etf?scope=aggr","/api/report/etf?scope=all","/api/inst-cobuy/observe","/api/picks"]:
    t=time.time()
    try:
        r=requests.get(B+u,timeout=60)
        j=r.json() if r.headers.get("content-type","").startswith("application/json") else {}
        out.append(f"{u} {r.status_code} {time.time()-t:.1f}s date={j.get('date')} model={j.get('model')} created={j.get('created')} notes={json.dumps(j.get('notes'),ensure_ascii=False)[:1500]} detail={j.get('detail')}")
    except Exception as e: out.append(f"{u} ERR {e}")
t=time.time()
try:
    r=requests.get(B+"/report/etf.pdf?scope=aggr",timeout=90,allow_redirects=False); out.append(f"pdf anon {r.status_code} {r.headers.get('location')} {r.headers.get('content-type')} {len(r.content)} {time.time()-t:.1f}s")
except Exception as e: out.append(f"pdf ERR {e} {time.time()-t:.1f}s")
open("debug/fut_result.txt","w").write("\n".join(out))
