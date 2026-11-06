import sys, sqlite3, datetime as dt
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
for d,c in (("2026-11-04",43000),("2026-11-05",41574.72)): st._q("INSERT INTO taiex_close VALUES (?,?)",(d,c))
st._q("INSERT INTO tx_close VALUES ('2026-11-04','202611',43400)"); st._q("INSERT INTO tx_close VALUES ('2026-11-05','202611',41882)")
for dd in (dt.date(2026,11,5), dt.date(2026,11,6)):
    print(dd, st.backfill_intraday(dd), st.intraday_incomplete(dd))
    r=st._q("SELECT * FROM fut_intraday WHERE d=? ORDER BY t",(dd.isoformat(),), fetch="all"); print(r[:2]); print(r[7:9]); print(r[-2:])
