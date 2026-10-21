import sys, csv, io, datetime as dt, requests, json, sqlite3, contextlib
sys.path.insert(0, "debug")
out=[]
try:
    r=requests.post("https://www.taifex.com.tw/cht/3/futContractsDateDown",headers={"User-Agent":"Mozilla/5.0"},timeout=60,
        data={"queryStartDate":"2026/10/14","queryEndDate":"2026/10/20","commodityId":""})
    rows=list(csv.reader(io.StringIO(r.content.decode("cp950","replace"))))
    out.append("HEADER: "+json.dumps(rows[0],ensure_ascii=False))
    for x in rows[1:]:
        if x and x[1].strip()=="臺股期貨": out.append(json.dumps(x,ensure_ascii=False))
except Exception as e: out.append(f"taifex ERR {e}")
try:
    r=requests.get("https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK",params={"date":"20261001","response":"json"},headers={"User-Agent":"Mozilla/5.0"},timeout=40)
    j=r.json(); out.append("FMTQIK fields "+json.dumps(j.get("fields"),ensure_ascii=False)); out+= [json.dumps(x,ensure_ascii=False) for x in (j.get("data") or [])[-5:]]
except Exception as e: out.append(f"twse ERR {e}")
# run new module end-to-end on sqlite for 60 days
try:
    import fut_flow_new as F
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
    st._save(st.fetch(dt.date(2026,7,1),dt.date(2026,10,20)))
    out.append("taiex rows: %s"%st.update_taiex([dt.date(2026,m,1) for m in (7,8,9,10)]))
    sp=st.spread("foreign",60)
    for d,a,b,t in list(zip(sp["dates"],sp["spread"],sp["sum20"],sp["taiex"]))[-25:]: out.append(f"{d} spread={a} sum20={b} taiex={t}")
    out.append("last "+json.dumps(sp["last"]))
except Exception as e:
    import traceback; out.append("module ERR "+traceback.format_exc())
open("debug/fut_result.txt","w").write("\n".join(out))
