import requests, datetime as d, re
UA={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"}
for u in ["https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
          "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/2024-11-01",
          "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/2020-01-01"]:
    try:
        r=requests.get(u,headers=UA,timeout=30); h=r.json()["fear_and_greed_historical"]["data"]
        print("CNN",u[-12:],r.status_code,len(h),d.datetime.utcfromtimestamp(h[0]["x"]/1000).date(),d.datetime.utcfromtimestamp(h[-1]["x"]/1000).date())
    except Exception as e: print("CNN",u[-12:],"ERR",str(e)[:120])
for ym in ["202611","202609","202606","202601","202511","202505","202411"]:
    u=f"https://www.taifex.com.tw/file/taifex/Dailydownload/vix/log2data/{ym}new.txt"
    try:
        r=requests.get(u,headers=UA,timeout=30); ok=b"<HTML" not in r.content[:200].upper()
        n=sum(1 for l in r.content.decode("big5","ignore").splitlines() if re.match(r"\d{8}\s",l))
        print("TWVIX",ym,r.status_code,"ok" if ok else "html",n,"rows")
    except Exception as e: print("TWVIX",ym,"ERR",str(e)[:100])
# 期交所其他可能的歷史來源
for u in ["https://www.taifex.com.tw/cht/7/vixMinNew","https://www.taifex.com.tw/cht/7/vixHistory","https://www.taifex.com.tw/file/taifex/Dailydownload/vix/vixhistory.csv"]:
    try:
        r=requests.get(u,headers=UA,timeout=30); print("ALT",u,r.status_code,len(r.content),r.content[:80])
    except Exception as e: print("ALT",u,"ERR",str(e)[:100])
