import sys, datetime as dt, sqlite3, contextlib, statistics as stt
sys.path.insert(0, "debug")
import fut_flow_new as F
out=[]
import traceback
try:
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
    for s,e in [(dt.date(2023,10,1),dt.date(2024,9,30)),(dt.date(2024,10,1),dt.date(2025,9,30)),(dt.date(2025,10,1),dt.date(2026,10,20))]:
        st._save(st.fetch(s,e))
    m=dt.date(2023,10,1); ms=[]
    while m<=dt.date(2026,10,1): ms.append(m); m=(m+dt.timedelta(days=32)).replace(day=1)
    st.update_taiex(ms)
    sp=st.spread("foreign",800)
    D=sp["dates"]; S=sp["spread"]; M=sp["sum20"]; T=sp["taiex"]
    ok=[i for i in range(len(D)) if T[i] and S[i] is not None]
    D=[D[i] for i in ok]; S=[S[i] for i in ok]; M=[M[i] for i in ok]; T=[T[i] for i in ok]
    n=len(D); out.append(f"期間 {D[0]} ~ {D[-1]}，{n} 個交易日")
    r=lambda i,k: T[i+k]/T[i]-1 if i+k<n else None   # i 收盤到 i+k 收盤
    r0=[T[i]/T[i-1]-1 if i else None for i in range(n)]
    def corr(a,b):
        p=[(x,y) for x,y in zip(a,b) if x is not None and y is not None]
        return stt.correlation([x for x,_ in p],[y for _,y in p]), len(p)
    c,k=corr(S,r0); out.append(f"同日：價差 vs 當天指數漲跌 相關係數 {c:.2f}（n={k}）")
    same=[(S[i]<0)==(r0[i]<0) for i in range(1,n) if S[i]!=0]; out.append(f"同日方向一致（價差負＝指數跌）{sum(same)/len(same):.0%}")
    for k in (1,5,20):
        c,nn=corr(S,[r(i,k) for i in range(n)]); out.append(f"每日價差 vs 之後 {k} 日漲跌 相關 {c:.2f}")
        hit=[(S[i]>0)==(r(i,k)>0) for i in range(n) if r(i,k) is not None and S[i]!=0]; up=[r(i,k)>0 for i in range(n) if r(i,k) is not None]
        big=sorted(abs(x) for x in S); th=big[int(len(big)*.8)]
        hb=[(S[i]>0)==(r(i,k)>0) for i in range(n) if r(i,k) is not None and abs(S[i])>=th]
        out.append(f"  價差正負猜之後 {k} 日方向：猜中 {sum(hit)/len(hit):.0%}（都猜漲 {sum(up)/len(up):.0%}）｜只看最大 20% 的日子（|價差|≥{th:.0f}）猜中 {sum(hb)/len(hb):.0%}，n={len(hb)}")
    out.append("")
    out.append("20 日合計分區間 → 之後 5／20 日報酬（平均、上漲比例）")
    bins=[(-9e9,-400),(-400,-250),(-250,-100),(-100,0),(0,100),(100,9e9)]
    for lo,hi in bins:
        idx=[i for i in range(n) if M[i] is not None and lo<=M[i]<hi]
        for k in (5,20):
            rs=[r(i,k) for i in idx if r(i,k) is not None]
            if rs: out.append(f"  20日合計 {lo:>6.0f}~{hi:<6.0f} n={len(rs):>3}｜之後 {k:>2} 日 平均 {stt.mean(rs):+.2%} 上漲 {sum(x>0 for x in rs)/len(rs):.0%}")
    allr={k:[r(i,k) for i in range(n) if r(i,k) is not None] for k in (5,20)}
    out.append(f"  全部日子：之後 5 日平均 {stt.mean(allr[5]):+.2%} 上漲 {sum(x>0 for x in allr[5])/len(allr[5]):.0%}｜20 日平均 {stt.mean(allr[20]):+.2%} 上漲 {sum(x>0 for x in allr[20])/len(allr[20]):.0%}")
    out.append("")
    # 轉折：20 日合計從 ≤ -300 回升（連 3 天上升）＝ 底部訊號
    ev=[]
    for i in range(23,n):
        if M[i-3] is not None and min(M[i-10:i-2] if None not in M[i-10:i-2] else [0])<=-300 and M[i]>M[i-1]>M[i-2]>M[i-3]:
            if not ev or i-ev[-1]>15: ev.append(i)
    for i in ev:
        out.append(f"  底部回升 {D[i]} 20日合計 {M[i]:.0f}｜之後 5 日 {r(i,5) and f'{r(i,5):+.1%}'}，20 日 {r(i,20) and f'{r(i,20):+.1%}'}")
    # 同期：20 日合計 vs 過去 20 日指數漲跌
    c,k=corr(M,[T[i]/T[i-20]-1 if i>=20 else None for i in range(n)]); out.append(f"20 日合計 vs 同期 20 日指數漲跌 相關 {c:.2f}")
    c,k=corr(M,[r(i,20) for i in range(n)]); out.append(f"20 日合計 vs 之後 20 日指數漲跌 相關 {c:.2f}")
    open("debug/fut_result.txt","w").write("\n".join(out))
except Exception:
    out.append(traceback.format_exc())
    open('debug/fut_result.txt','w').write('\n'.join(out))
