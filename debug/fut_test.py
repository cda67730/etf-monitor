import sys, datetime as dt, json, requests, collections
sys.path.insert(0, "debug")
import fut_flow_new as F
out=[]; rows=[]
for s,e in [(dt.date(2024,10,21),dt.date(2025,10,19)),(dt.date(2025,10,20),dt.date(2026,10,20))]:
    rows+=F.FutStore.fetch(s,e)
PRODS={"美國費城半導體期貨":("^SOX","費半"),"美國那斯達克100期貨":("^NDX","那指"),"美國道瓊期貨":("^DJI","道瓊"),"美國標普500期貨":("^GSPC","標普")}
fut=collections.defaultdict(dict)
for r in rows:
    if r[1] in PRODS: fut[(r[1],r[2])][r[0]]=r[6]
us={}
for p,(sym,_) in PRODS.items():
    j=requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}",params={"range":"2y","interval":"1d"},headers={"User-Agent":"Mozilla/5.0"},timeout=30).json()
    res=j["chart"]["result"][0]; ts=res["timestamp"]; cl=res["indicators"]["quote"][0]["close"]
    days=[(dt.datetime.utcfromtimestamp(t)-dt.timedelta(hours=4)).date().isoformat() for t in ts]
    us[p]=[(d,c) for d,c in zip(days,cl) if c]
def ret(prod, d, k):
    """台灣日期 d 當晚起算 k 個美國交易日的報酬（k=1 當晚）"""
    L=us[prod]; i=next((i for i,(x,_) in enumerate(L) if x>=d),None)
    if i is None or i==0 or L[i][0]!=d or i+k-1>=len(L): return None
    return L[i+k-1][1]/L[i-1][1]-1
def run(prod,inst,name,cond):
    f=fut[(prod,inst)]; ds=sorted(f); res={1:[0,0,0],5:[0,0,0]}
    for i in range(5,len(ds)):
        d=ds[i]; sig=cond(f,ds,i)
        if not sig: continue
        for k in (1,5):
            r=ret(prod,d,k)
            if r is None: continue
            res[k][0]+=1; res[k][1]+=(sig>0)==(r>0); res[k][2]+=r>0
    a,b=res[1],res[5]
    if a[0]<8: return f"  {name}: 天數 {a[0]}（太少）"
    return f"  {name}: 天數 {a[0]}｜當晚猜中 {a[1]/a[0]:.0%}（當晚漲 {a[2]/a[0]:.0%}）｜5 日猜中 {b[1]/max(b[0],1):.0%}（5 日漲 {b[2]/max(b[0],1):.0%}）"
dl=lambda f,ds,i,j=0: f[ds[i-j]]-f[ds[i-j-1]]
conds={
 "淨部位翻正負（淨空→淨多或反過來）": lambda f,ds,i: (1 if f[ds[i]]>0 else -1) if (f[ds[i]]>0)!=(f[ds[i-1]]>0) and f[ds[i]]!=0 else 0,
 "連 3 天同向後反轉": lambda f,ds,i: (1 if dl(f,ds,i)>0 else -1) if dl(f,ds,i)*dl(f,ds,i,1)<0 and dl(f,ds,i,1)*dl(f,ds,i,2)>0 and dl(f,ds,i,2)*dl(f,ds,i,3)>0 else 0,
}
for pct in (10,20,30,50):
    conds[f"單日變動 ≥ 前日部位 {pct}%"]=(lambda p: lambda f,ds,i: (1 if dl(f,ds,i)>0 else -1) if abs(f[ds[i-1]])>=50 and abs(dl(f,ds,i))>=abs(f[ds[i-1]])*p/100 else 0)(pct)
for prod,(sym,short) in PRODS.items():
    for inst,nm in (("外資及陸資","外資"),("自營商","自營")):
        out.append(f"## {short}・{nm}")
        for name,c in conds.items(): out.append(run(prod,inst,name,c))
open("debug/fut_result.txt","w").write("\n".join(out))
