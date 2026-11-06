import requests, json
UA={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"}
d="20261105"
r=requests.get("https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN",params={"date":d,"selectType":"ALL","response":"json"},headers=UA,timeout=30)
j=r.json(); print("TWSE", r.status_code, j.get("stat"), list(j.keys()))
for t in j.get("tables",[]):
    print("TABLE", t.get("title"), t.get("fields"), len(t.get("data") or []), (t.get("data") or [])[:3], t.get("groups"))
for u,p in [("https://www.tpex.org.tw/www/zh-tw/margin/balance",{"date":"2026/11/05","response":"json"}),
            ("https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php",{"l":"zh-tw","o":"json","d":"115/11/05"})]:
    try:
        r=requests.get(u,params=p,headers=UA,timeout=30); j=r.json(); print("TPEX",u[-40:],r.status_code,list(j.keys()))
        for t in (j.get("tables") or [])[:3]:
            print(" T",t.get("title"),t.get("fields"),len(t.get("data") or []),(t.get("data") or [])[:2], t.get("summary"))
        if "aaData" in j: print(" aa", j.get("iTotalRecords"), j["aaData"][:2], j.get("tfootData_one"), j.get("tfootData_two"))
    except Exception as e: print("TPEX ERR",u[-40:],repr(e)[:150])
