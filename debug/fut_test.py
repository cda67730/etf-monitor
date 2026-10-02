import sys, csv, datetime as dt, time
sys.path.insert(0, "debug")
import fut_flow as F
today = dt.date(2026, 10, 2)
s = today - dt.timedelta(days=F.BACKFILL_DAYS)
rows = []
while s <= today:
    e = min(s + dt.timedelta(days=364), today)
    r = F.FutStore.fetch(s, e); print(s, e, len(r)); rows += r
    s = e + dt.timedelta(days=1); time.sleep(1)
names = {p for p, _ in F.PRODUCTS}
keep = [r for r in rows if r[1] in names]
print("kept", len(keep), "products seen", sorted({r[1] for r in keep}), "missing", names - {r[1] for r in rows})
with open("debug/fut_rows.csv", "w", newline="") as f:
    csv.writer(f).writerows(keep)
