import requests, io, zipfile, collections
UA={"User-Agent":"Mozilla/5.0"}
for day in ["2026_11_06","2026_11_05"]:
    r=requests.get(f"https://www.taifex.com.tw/file/taifex/Dailydownload/DailydownloadCSV/Daily_{day}.zip",headers=UA,timeout=120)
    z=zipfile.ZipFile(io.BytesIO(r.content)); c=collections.Counter(); mx={}
    for l in z.read(z.namelist()[0]).decode("cp950","replace").splitlines()[1:]:
        p=l.split(",")
        if len(p)>4 and p[1].strip()=="TX" and p[2].strip()=="202611":
            k=(p[0], p[3][:2]); c[k]+=1
    print(day, r.headers.get("Last-Modified"), sorted(c.items()))
