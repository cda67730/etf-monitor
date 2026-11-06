import requests, csv, io, json, datetime as d
UA={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"}
def show(name, f):
    try: print(name, f())
    except Exception as e: print(name, "ERR", repr(e)[:200])
# 1 TWSE MIS 加權指數即時
show("TWSE_MIS", lambda: str(requests.get("https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0",headers={**UA,"Referer":"https://mis.twse.com.tw/stock/index"},timeout=20).json().get("msgArray",[{}])[0])[:400])
# 2 TAIFEX MIS 台指期報價
def tx():
    r=requests.post("https://mis.taifex.com.tw/futures/api/getQuoteList",headers={**UA,"Referer":"https://mis.taifex.com.tw/futures/RegularSession/EquityIndices/FuturesDomestic/"},timeout=20,
        json={"MarketType":"0","SymbolType":"F","KindID":"1","CID":"TXF","ExpireMonth":"","RowSize":"全部","PageNo":"","SortColumn":"","AscDesc":"A"})
    q=(r.json().get("RtData") or {}).get("QuoteList") or []
    return r.status_code, len(q), [ {k:x.get(k) for k in ("SymbolID","DispCName","CLastPrice","CRefPrice","CDate","CTime","SpotPrice","CBestBidPrice")} for x in q[:4]]
show("TAIFEX_MIS", tx)
# 3 期交所每日行情 TX（收盤價、到期月份）
def fd():
    e=d.date.today(); s=e-d.timedelta(days=10)
    r=requests.post("https://www.taifex.com.tw/cht/3/futDataDown",headers=UA,timeout=60,data={"down_type":"1","commodity_id":"TX","commodity_id2":"","queryStartDate":s.strftime("%Y/%m/%d"),"queryEndDate":e.strftime("%Y/%m/%d")})
    rows=list(csv.reader(io.StringIO(r.content.decode("cp950","replace"))))
    return rows[0], rows[1:6]
show("FUT_DAILY", fd)
# 4 證交所每 5 秒指數（盤後可抓當天）
show("MI_5MINS", lambda: (lambda j:(j.get("stat"), j.get("fields"), (j.get("data") or [])[:2], len(j.get("data") or [])))(requests.get("https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_INDEX?response=json&date="+(d.date.today()-d.timedelta(days=1)).strftime("%Y%m%d"),headers=UA,timeout=20).json()))
show("MI_5MINS_today", lambda: (lambda j:(j.get("stat"), len(j.get("data") or []), (j.get("data") or [])[-1:]))(requests.get("https://www.twse.com.tw/rwd/zh/TAIEX/MI_5MINS_INDEX?response=json",headers=UA,timeout=20).json()))
