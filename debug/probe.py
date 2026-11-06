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
print("live", F.FutStore.fetch_live())
print("poll", st.poll_live(force=True))
t=dt.date.today(); m=F.FutStore.fetch_tx_close(t-dt.timedelta(days=40), t-dt.timedelta(days=13))
print("txclose", len(m), sorted(m.items())[:3], sorted(m.items())[-2:])
print("basis_upd", st.update_basis(t) if False else "skip")
