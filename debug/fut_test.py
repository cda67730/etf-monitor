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
    s=dt.date(2023,11,1)
    while s<=dt.date(2026,10,20):
        e=min(s+dt.timedelta(days=90),dt.date(2026,10,20))
        import time
        for t in range(5):
            try: st._save(st.fetch(s,e)); break
            except Exception as ex:
                time.sleep(10)
                if t==4: out.append(f"skip {s}~{e}: {str(ex)[:60]}")
        s=e+dt.timedelta(days=1)
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
    def test(name, sig):
        for k in (1,5,20):
            idx=[i for i in range(n) if sig(i) is not None and r(i,k) is not None]
            if not idx: continue
            hit=[(sig(i)>0)==(r(i,k)>0) for i in idx]
            up=[i for i in idx if sig(i)>0]; dn=[i for i in idx if sig(i)<0]
            mu=lambda L: f"{stt.mean([r(i,k) for i in L]):+.2%}（漲 {sum(r(i,k)>0 for i in L)/len(L):.0%}，n={len(L)}）" if L else "-"
            out.append(f"{name}｜之後 {k:>2} 日 猜中 {sum(hit)/len(hit):.0%}｜轉多/向上時 {mu(up)}｜轉空/向下時 {mu(dn)}")
    g=lambda i,j: M[i] is not None and i-j>=0 and M[i-j] is not None
    test("20日合計 1日方向", lambda i: (M[i]-M[i-1]) if g(i,1) and M[i]!=M[i-1] else None)
    test("20日合計 5日方向", lambda i: (M[i]-M[i-5]) if g(i,5) and M[i]!=M[i-5] else None)
    test("20日合計 翻正/翻負", lambda i: (1 if M[i]>0 else -1) if g(i,1) and (M[i]>0)!=(M[i-1]>0) else None)
    test("每日價差 由負翻正/由正翻負", lambda i: (1 if S[i]>0 else -1) if i and S[i]*S[i-1]<0 else None)
    test("每日價差 連3天同號", lambda i: (1 if S[i]>0 else -1) if i>=2 and S[i]*S[i-1]>0 and S[i-1]*S[i-2]>0 and (i<3 or S[i-3]*S[i]<=0) else None)
    out.append("===Y2026===")
    Y=[i for i in range(n) if D[i]>="2026-01-01"]
    out.append(f"2026 共 {len(Y)} 天；指數 {T[Y[0]]:.0f} → {T[Y[-1]]:.0f}")
    def stat(L,k):
        rs=[r(i,k) for i in L if r(i,k) is not None]
        return f"{stat_mean(rs)} 漲{sum(x>0 for x in rs)/len(rs):.0%} n={len(rs)}" if rs else "-"
    stat_mean=lambda rs: f"{stt.mean(rs):+.2%}"
    out.append("基準｜"+"｜".join(f"{k}日 {stat(Y,k)}" for k in (1,5,20)))
    def hit(name,sig):
        parts=[]
        for k in (1,5,20):
            L=[i for i in Y if sig(i) is not None and r(i,k) is not None]
            if L: parts.append(f"{k}日 猜中 {sum((sig(i)>0)==(r(i,k)>0) for i in L)/len(L):.0%}")
        up=[i for i in Y if sig(i) is not None and sig(i)>0]; dn=[i for i in Y if sig(i) is not None and sig(i)<0]
        out.append(f"{name}｜"+"｜".join(parts)+f"｜正/向上後 5日 {stat(up,5)}｜負/向下後 5日 {stat(dn,5)}")
    hit("每日價差正負", lambda i: S[i] if S[i] else None)
    hit("20日合計正負", lambda i: M[i] if M[i] else None)
    hit("20日合計1日方向", lambda i: (M[i]-M[i-1]) if M[i] is not None and M[i-1] is not None and M[i]!=M[i-1] else None)
    hit("20日合計5日方向", lambda i: (M[i]-M[i-5]) if M[i] is not None and M[i-5] is not None and M[i]!=M[i-5] else None)
    c=stt.correlation([S[i] for i in Y[:-1]],[r(i,1) for i in Y[:-1]]); out.append(f"每日價差 vs 隔日 相關 {c:.2f}")
    c=stt.correlation([S[i] for i in Y],[r0[i] for i in Y]); out.append(f"每日價差 vs 當日 相關 {c:.2f}")
    for L in (3,5):
        for kind in ("底","頂"):
            ev=[]
            for i in Y:
                w=M[i-L-2:i+1]
                if None in w: continue
                d=[w[j+1]-w[j] for j in range(len(w)-1)]
                ok=(all(x<0 for x in d[:L]) and all(x>0 for x in d[L:])) if kind=="底" else (all(x>0 for x in d[:L]) and all(x<0 for x in d[L:]))
                if ok and (not ev or i-ev[-1]>5): ev.append(i)
            out.append(f"連{'跌' if kind=='底' else '漲'}{L}天後轉{'上' if kind=='底' else '下'}｜"+"｜".join(f"{k}日 {stat(ev,k)}" for k in (5,20))+"｜"+",".join(D[i][5:] for i in ev))
    # 每月：月底 20 日合計 vs 下月漲跌
    out.append("月份｜月底20日合計｜當月指數")
    mo={}
    for i in Y: mo.setdefault(D[i][:7],[]).append(i)
    for m,L in mo.items(): out.append(f"  {m}｜{M[L[-1]]:.0f}｜{T[L[-1]]/T[L[0]-1]-1:+.1%}")
    out.append("===YEND===")
    out.append("===TURN===")
    # 轉折：20 日合計連跌 >=L 天後連漲 2 天（觸底轉上），或連漲 >=L 天後連跌 2 天（觸頂轉下）
    base={k:stt.mean([r(i,k) for i in range(n) if r(i,k) is not None]) for k in (1,5,10,20)}
    baseu={k:sum(r(i,k)>0 for i in range(n) if r(i,k) is not None)/len([1 for i in range(n) if r(i,k) is not None]) for k in (1,5,10,20)}
    out.append("全部日子 基準：" + "｜".join(f"{k}日 {base[k]:+.2%} 漲{baseu[k]:.0%}" for k in (1,5,10,20)))
    for L in (3,5):
        for kind in ("底","頂"):
            ev=[]
            for i in range(L+2,n):
                w=M[i-L-2:i+1]
                if None in w: continue
                d=[w[j+1]-w[j] for j in range(len(w)-1)]
                pre,post=d[:L],d[L:]
                ok=(all(x<0 for x in pre) and all(x>0 for x in post)) if kind=="底" else (all(x>0 for x in pre) and all(x<0 for x in post))
                if ok and (not ev or i-ev[-1]>5): ev.append(i)
            parts=[]
            for k in (1,5,10,20):
                rs=[r(i,k) for i in ev if r(i,k) is not None]
                if rs: parts.append(f"{k}日 {stt.mean(rs):+.2%} 漲{sum(x>0 for x in rs)/len(rs):.0%}")
            out.append(f"20日合計 連{'跌' if kind=='底' else '漲'}{L}天後轉{'上' if kind=='底' else '下'}（{kind}） n={len(ev)}｜"+"｜".join(parts))
            if L==5:
                for i in ev:
                    out.append(f"   {D[i]} 合計 {M[i]:.0f}｜5日 {r(i,5) and format(r(i,5),'+.1%')}｜20日 {r(i,20) and format(r(i,20),'+.1%')}")
    out.append("===END===")
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
