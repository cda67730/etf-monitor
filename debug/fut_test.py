import requests, re, json
H={"User-Agent":"Mozilla/5.0"}
out=[]
try:
    r=requests.get("https://etf-monitor-production.up.railway.app/api/etfs?scope=every",headers=H,timeout=60)
    out.append("== registry "+str(r.status_code)); out.append(r.text[:20000])
except Exception as e: out.append("registry ERR "+str(e))
for mode in ("2","4"):
    r=requests.get(f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}",headers=H,timeout=60)
    t=r.content.decode("big5",errors="replace")
    rows=re.findall(r"<tr><td bgcolor=#FAFAD2>(.*?)</td><td bgcolor=#FAFAD2>(.*?)</td><td bgcolor=#FAFAD2>(.*?)</td><td bgcolor=#FAFAD2>(.*?)</td><td bgcolor=#FAFAD2>(.*?)</td><td bgcolor=#FAFAD2>(.*?)</td>",t)
    out.append(f"== isin mode {mode} rows={len(rows)}")
    for a,isin,listed,market,ind,cfi in rows:
        code,_,name=a.replace("　"," ").partition(" ")
        code=code.strip(); name=name.strip()
        if re.fullmatch(r"00\d{3,4}[A-Z]",code) and (code.endswith("A") or "主動" in name):
            out.append(f"{code}\t{name}\t{listed}\t{market}\t{cfi}")
open("debug/fut_result.txt","w").write("\n".join(out))
