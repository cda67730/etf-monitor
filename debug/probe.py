import sys, sqlite3, datetime as dt, contextlib
sys.path.insert(0, "debug")
import margin_new as M
class DB:
    db_type="sqlite"
    def __init__(s): s.c=sqlite3.connect(":memory:",check_same_thread=False); s.c.row_factory=sqlite3.Row
    def execute_query(s,sql,p=(),fetch="none"):
        cur=s.c.execute(sql,p); s.c.commit()
        if fetch=="one": r=cur.fetchone(); return dict(r) if r else None
        if fetch=="all": return [dict(r) for r in cur.fetchall()]
st=M.MarginStore(DB())
print(st.update(backfill_days=40))
t=st.totals(60); print("totals", len(t["dates"]), t["dates"][-3:], t["repay"][-3:], t["amt"][-3:], t["bal_amt"][-1], t["partial"])
for p in (1,5):
    r=st.ranking(None,p); print("rank",p,r["date"],r["start"],len(r["down"]),len(r["repay"]))
    print(" down", [(x["id"],x["name"],x["chg"],x["chg_pct"],x["repay"]) for x in r["down"][:4]])
    print(" rep", [(x["id"],x["name"],x["repay"],x["ratio"],x["repay_pct"]) for x in r["repay"][:4]])
print("skip", st._q("SELECT * FROM margin_skip", fetch="all"))
