import requests, csv, io, json
H={"User-Agent":"Mozilla/5.0"}
out=[]
for cid in ("MTX","TX"):
    r=requests.post("https://www.taifex.com.tw/cht/3/futDataDown",headers=H,timeout=120,
        data={"down_type":"1","commodity_id":cid,"commodity_id2":"","queryStartDate":"2026/09/28","queryEndDate":"2026/10/02"})
    t=r.content.decode("cp950",errors="replace"); rows=list(csv.reader(io.StringIO(t)))
    out.append(f"== {cid} {r.status_code} {r.headers.get('content-type')} rows={len(rows)}")
    out += [json.dumps(x,ensure_ascii=False) for x in rows[:25]]
# 一年查詢可否
r=requests.post("https://www.taifex.com.tw/cht/3/futDataDown",headers=H,timeout=120,
    data={"down_type":"1","commodity_id":"MTX","commodity_id2":"","queryStartDate":"2025/10/03","queryEndDate":"2026/10/02"})
t=r.content.decode("cp950",errors="replace"); rows=list(csv.reader(io.StringIO(t)))
out.append(f"== MTX 1y {r.status_code} rows={len(rows)} first={rows[1][:3] if len(rows)>1 else t[:200]} last={rows[-1][:3] if rows else ''}")
r=requests.post("https://www.taifex.com.tw/cht/3/futDataDown",headers=H,timeout=120,
    data={"down_type":"1","commodity_id":"MTX","commodity_id2":"","queryStartDate":"2026/01/01","queryEndDate":"2026/10/02"})
t=r.content.decode("cp950",errors="replace"); rows=list(csv.reader(io.StringIO(t)))
out.append(f"== MTX 9m {r.status_code} rows={len(rows)} head={t[:200]!r}")
open("debug/fut_result.txt","w").write("\n".join(out))
