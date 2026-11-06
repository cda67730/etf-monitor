import requests, time, datetime as d
UA={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"}
now=int(time.time()); frm=now-800*86400
for sym in ["TWS:VIXTWN:INDEX","TWS:TAIEXVIX:INDEX","TWS:VIX:INDEX","TWF:VIX:INDEX","TWS:TWVIX:INDEX"]:
    u=f"https://ws.api.cnyes.com/ws/api/v1/charting/history?symbol={sym}&resolution=D&quote=1&from={now}&to={frm}"
    try:
        r=requests.get(u,headers=UA,timeout=30); j=r.json(); dd=(j.get("data") or {})
        t=dd.get("t") or []; c=dd.get("c") or []
        print("CNYES",sym,r.status_code,len(t), (d.date.fromtimestamp(min(t)),d.date.fromtimestamp(max(t)),c[:3]) if t else str(j)[:150])
    except Exception as e: print("CNYES",sym,"ERR",str(e)[:120])
for u in ["https://www.taifex.com.tw/cht/7/vixDaily3MNew"]:
    r=requests.get(u,headers=UA,timeout=30); import re
    print("3M links", sorted(set(re.findall(r'href=["\']([^"\']*(?:txt|csv|zip)[^"\']*)', r.text)))[:20])
