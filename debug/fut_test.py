import requests, io, csv, json
H={"User-Agent":"Mozilla/5.0"}
out=[]
def t(name, url, data):
    try:
        r=requests.post(url,data=data,headers=H,timeout=60)
        raw=r.content
        for enc in ("utf-8-sig","big5","cp950"):
            try: txt=raw.decode(enc); break
            except Exception: pass
        out.append(f"== {name} {r.status_code} {r.headers.get('content-type')} len={len(raw)} enc={enc}")
        rows=list(csv.reader(io.StringIO(txt)))
        out.append(json.dumps(rows[:1],ensure_ascii=False))
        out.append("\n".join(json.dumps(x,ensure_ascii=False) for x in rows[1:40]))
        out.append(f"rows={len(rows)} dates={sorted(set(x[0] for x in rows[1:] if x))[:3]}..{sorted(set(x[0] for x in rows[1:] if x))[-3:]}")
        names=sorted(set(x[1] for x in rows[1:] if len(x)>1)); out.append("products="+json.dumps(names,ensure_ascii=False))
    except Exception as e:
        out.append(f"== {name} ERR {e}")
U="https://www.taifex.com.tw/cht/3/futContractsDateDown"
t("1day", U, {"queryStartDate":"2026/10/01","queryEndDate":"2026/10/01","commodityId":""})
t("1month", U, {"queryStartDate":"2026/09/01","queryEndDate":"2026/09/30","commodityId":""})
t("1year", U, {"queryStartDate":"2025/10/01","queryEndDate":"2026/10/01","commodityId":""})
t("tx_1year", U, {"queryStartDate":"2025/10/01","queryEndDate":"2026/10/01","commodityId":"TXF"})
open("debug/fut_result.txt","w").write("\n".join(out)); print("\n".join(out)[:3000])
