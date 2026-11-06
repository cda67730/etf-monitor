import sys, sqlite3, datetime as dt, time
sys.path.insert(0, "debug")
import fut_flow_new as F
class DB:
    db_type="sqlite"
    def __init__(s): s.c=sqlite3.connect(":memory:",check_same_thread=False); s.c.row_factory=sqlite3.Row
    def execute_query(s,sql,p=(),fetch="none"):
        cur=s.c.execute(sql,p); s.c.commit()
        if fetch=="one": r=cur.fetchone(); return dict(r) if r else None
        if fetch=="all": return [dict(r) for r in cur.fetchall()]
st=F.FutStore(DB())
st._q("INSERT INTO taiex_close VALUES ('2026-11-05', 41574.72)"); st._q("INSERT INTO tx_close VALUES ('2026-11-05','202611',41882)")
t=time.time(); n=st.backfill_intraday(dt.date(2026,11,6)); print("n",n, round(time.time()-t,1),"s")
r=st._q("SELECT * FROM fut_intraday ORDER BY t", fetch="all"); print(r[:3]); print(r[5:8]); print(r[-3:])
