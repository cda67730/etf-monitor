import sys, datetime as dt, json, requests, collections
sys.path.insert(0, "debug")
import fut_flow_new as F
out=[]
rows=[]
for s,e in [(dt.date(2024,10,21),dt.date(2025,10,19)),(dt.date(2025,10,20),dt.date(2026,10,20))]:
    rows+=F.FutStore.fetch(s,e)
PRODS={"美國費城半導體期貨":"^SOX","美國那斯達克100期貨":"^NDX","美國道瓊期貨":"^DJI","美國標普500期貨":"^GSPC"}
fut=collections.defaultdict(dict)   # (prod,inst) -> {d: (net_trade, net_oi)}
for r in rows:
    if r[1] in PRODS: fut[(r[1],r[2])][r[0]]=(r[3],r[6])
us={}
for p,sym in PRODS.items():
    j=requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",params={"range":"2y","interval":"1d"},headers={"User-Agent":"Mozilla/5.0"},timeout=30).json()
    res=j["chart"]["result"][0]; ts=res["timestamp"]; cl=res["indicators"]["quote"][0]["close"]
    # 美國交易日（紐約時間）
    days=[(dt.datetime.utcfromtimestamp(t)-dt.timedelta(hours=4)).date().isoformat() for t in ts]
    ret={}
    for i in range(1,len(days)):
        if cl[i] and cl[i-1]: ret[days[i]]=cl[i]/cl[i-1]-1
    us[p]=ret
out.append("US last days: "+json.dumps({p:sorted(us[p])[-3:] for p in us},ensure_ascii=False))
def evalset(prod, inst, kind, thr=0):
    f=fut[(prod,inst)]; ds=sorted(f); hit=n=up=0
    for i,d in enumerate(ds):
        if d not in us[prod]: continue
        if kind=="trade": s=f[d][0]
        else:
            if i==0: continue
            s=f[d][1]-f[ds[i-1]][1]
        if abs(s)<=thr: continue
        r=us[prod][d]; n+=1; up+=r>0
        hit+= (s>0)==(r>0)
    return n,hit,up
for prod in PRODS:
    for inst in ("外資及陸資","自營商","投信"):
        if (prod,inst) not in fut: continue
        line=[]
        for kind in ("trade","oi"):
            vals=[abs(v[0] if kind=="trade" else 0) for v in fut[(prod,inst)].values()]
            n,h,u=evalset(prod,inst,kind)
            if n==0: continue
            # 只看較大的變動（絕對值前 1/3）
            f=fut[(prod,inst)]; ds=sorted(f)
            mags=sorted(abs(f[d][0]) if kind=="trade" else abs(f[d][1]-f[ds[i-1]][1]) for i,d in enumerate(ds) if i)
            thr=mags[int(len(mags)*2/3)] if mags else 0
            n2,h2,u2=evalset(prod,inst,kind,thr)
            line.append(f"{'當日交易淨額' if kind=='trade' else '未平倉淨額變化'}: n={n} 猜中 {h/n:.1%}（當晚上漲比例 {u/n:.1%}）｜大變動 n={n2} 猜中 {h2/n2:.1%}" if n2 else "")
        out.append(f"{prod} {inst}: "+" ／ ".join(line))
# 期間
out.append("期間 "+min(fut[("美國費城半導體期貨","外資及陸資")])+" ~ "+max(fut[("美國費城半導體期貨","外資及陸資")]))
open("debug/fut_result.txt","w").write("\n".join(out))
