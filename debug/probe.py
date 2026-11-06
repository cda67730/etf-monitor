import requests, io, zipfile, datetime as d, time
UA={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"}
for day in [d.date.today(), d.date.today()-d.timedelta(days=1)]:
    u=f"https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/Daily_{day:%Y_%m_%d}.zip"
    t=time.time()
    try:
        r=requests.get(u,headers=UA,timeout=120); print(u[-25:], r.status_code, len(r.content), round(time.time()-t,1),"s", r.content[:4])
        if r.content[:2]==b"PK":
            z=zipfile.ZipFile(io.BytesIO(r.content)); n=z.namelist(); print(n)
            raw=z.read(n[0]); txt=raw.decode("cp950","replace").splitlines()
            print(len(txt)); print("\n".join(txt[:3]))
            tx=[l for l in txt if l.split(",")[1:2]==["TX      "] or (len(l.split(","))>1 and l.split(",")[1].strip()=="TX")]
            print("TX lines", len(tx)); print("\n".join(tx[:3])); print("\n".join(tx[-2:]))
    except Exception as e: print("ERR", repr(e)[:200])
