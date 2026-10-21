import sys, datetime as dt, json, sqlite3, contextlib
sys.path.insert(0, "debug")
out=[]
try:
    import fut_flow_new as F
    import matplotlib; matplotlib.use("Agg"); import matplotlib.pyplot as plt
    class DB:
        db_type="sqlite"
        def __init__(s): s.c=sqlite3.connect(":memory:",check_same_thread=False); s.c.row_factory=sqlite3.Row
        def execute_query(s,sql,p=(),fetch="none"):
            cur=s.c.execute(sql,p); s.c.commit()
            if fetch=="one": r=cur.fetchone(); return dict(r) if r else None
            if fetch=="all": return [dict(r) for r in cur.fetchall()]
        @contextlib.contextmanager
        def get_connection(s): yield s.c
    st=F.FutStore(DB())
    st._save(st.fetch(dt.date(2026,3,15),dt.date(2026,10,20)))
    st.update_taiex([dt.date(2026,m,1) for m in range(4,11)])
    sp=st.spread("foreign",160)
    D,S,M,T=sp["dates"],sp["spread"],sp["sum20"],sp["taiex"]
    for i,d in enumerate(D):
        if d>="2026-05-15": out.append(f"{d} 日={S[i]} 20日={M[i]} 指數={T[i]}")
    k=min(range(len(M)),key=lambda i:M[i] if M[i] is not None else 9e9); out.append(f"MIN sum20 {D[k]} {M[k]}")
    # 仿節目版面
    i0=next(i for i,d in enumerate(D) if d>="2026-05-18")
    fig,(a1,a3)=plt.subplots(2,1,figsize=(12,8),gridspec_kw={"height_ratios":[2,1]})
    a1.bar(range(len(D)-i0),[m or 0 for m in M[i0:]],color=["#2ca02c" if (m or 0)<0 else "#d62728" for m in M[i0:]])
    a2=a1.twinx(); a2.plot(range(len(D)-i0),T[i0:],color="k"); a1.set_xticks(range(0,len(D)-i0,10)); a1.set_xticklabels([D[i0+j][5:] for j in range(0,len(D)-i0,10)],rotation=30)
    a1.set_title("20-day sum (bars) & TAIEX")
    j0=next(i for i,d in enumerate(D) if d>="2026-08-06")
    a3.bar(range(len(D)-j0),S[j0:],color=["#2ca02c" if s<0 else "#d62728" for s in S[j0:]]); a3.set_xticks(range(0,len(D)-j0,5)); a3.set_xticklabels([D[j0+j][5:] for j in range(0,len(D)-j0,5)],rotation=30); a3.set_title("daily spread")
    plt.tight_layout(); plt.savefig("debug/spread.png",dpi=80)
except Exception:
    import traceback; out.append(traceback.format_exc())
open("debug/fut_result.txt","w").write("\n".join(out))
