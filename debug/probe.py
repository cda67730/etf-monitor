import requests, re
UA={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"}
r=requests.get("https://www.taifex.com.tw/cht/7/vixMinNew",headers=UA,timeout=30)
t=r.text
print("title", re.findall(r"<title>(.*?)</title>", t, re.S)[:1])
links=sorted(set(re.findall(r'(?:href|src|value)=["\']([^"\']*(?:vix|VIX|log2|zip|txt|csv)[^"\']*)["\']', t)))
print(len(links)); print("\n".join(links[:80]))
for kw in ["歷史","下載","log2data","zip"]:
    for m in re.finditer(kw, t):
        print(kw, re.sub(r"\s+"," ",t[max(0,m.start()-150):m.start()+150])); break
