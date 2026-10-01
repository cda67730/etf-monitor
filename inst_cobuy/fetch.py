# -*- coding: utf-8 -*-
"""抓證交所 T86（上市）+ 櫃買中心（上櫃）三大法人買賣超，存成 data/YYYYMMDD.csv（單位：股）
用法：python fetch.py [往回補幾個交易日，預設 60]
"""
import csv, datetime as dt, json, os, re, sys, time
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, 'data')
HOLI = os.path.join(DATA, '_no_trading.txt')
UA = {'User-Agent': 'Mozilla/5.0 (inst-cobuy daily job)'}
TW = dt.timezone(dt.timedelta(hours=8))
SLEEP = 4


def get_json(url, params):
    """GET 並解析 JSON；失敗時重試，最後把狀態碼與內容開頭印出來方便除錯"""
    last = None
    for i in range(3):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=30)
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError:
                    last = f'非 JSON 回應：{r.text[:200]!r}'
            else:
                last = f'HTTP {r.status_code}：{r.text[:200]!r}'
        except requests.RequestException as e:
            last = f'連線錯誤：{e}'
        time.sleep(SLEEP * (i + 2))
    raise RuntimeError(f'{url} {params} 失敗 → {last}')


def num(s):
    s = str(s).replace(',', '').strip()
    try:
        return int(float(s))
    except ValueError:
        return 0


def pick(fields, *keys, exclude=()):
    """回傳第一個名稱同時包含所有 keys、且不含 exclude 的欄位 index"""
    for i, f in enumerate(fields):
        f = re.sub(r'<[^>]+>|\s', '', str(f))
        if all(k in f for k in keys) and not any(x in f for x in exclude):
            return i
    return None


def twse(day):
    url = 'https://www.twse.com.tw/rwd/zh/fund/T86'
    j = get_json(url, {'date': day.strftime('%Y%m%d'), 'selectType': 'ALLBUT0999', 'response': 'json'})
    if j.get('stat') != 'OK' or not j.get('data'):
        return None
    f = j['fields']
    i_code, i_name = 0, 1
    i_fx = pick(f, '外陸資買賣超')
    i_fxd = pick(f, '外資自營商買賣超')
    i_it = pick(f, '投信買賣超')
    i_dl = pick(f, '自營商買賣超股數', exclude=('自行', '避險', '外資'))
    if None in (i_fx, i_it, i_dl):
        raise RuntimeError(f'TWSE 欄位對不上：{f}')
    out = []
    for row in j['data']:
        fx = num(row[i_fx]) + (num(row[i_fxd]) if i_fxd is not None else 0)
        out.append([row[i_code].strip(), row[i_name].strip(), '上市', fx, num(row[i_it]), num(row[i_dl])])
    return out


def _tpex_rows_new(day):
    url = 'https://www.tpex.org.tw/www/zh-tw/insti/dailyTrade'
    j = get_json(url, {'type': 'Daily', 'sect': 'EW', 'date': day.strftime('%Y/%m/%d'), 'response': 'json'})
    t = (j.get('tables') or [{}])[0]
    return t.get('fields'), t.get('data')


def _tpex_rows_old(day):
    roc = f'{day.year - 1911}/{day:%m/%d}'
    url = 'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php'
    j = get_json(url, {'l': 'zh-tw', 'se': 'EW', 't': 'D', 'd': roc, 'o': 'json'})
    return None, j.get('aaData') or j.get('tables', [{}])[0].get('data')


def tpex(day):
    try:
        f, data = _tpex_rows_new(day)
    except Exception as e:
        print(f'  櫃買新版 API 失敗，改用舊版：{e}')
        f, data = None, None
    if not data:
        time.sleep(SLEEP)
        f, data = _tpex_rows_old(day)
    if not data:
        return None
    # 櫃買欄位：0代號 1名稱 | 2-4 外陸資(不含外資自營商) | 5-7 外資自營商 | 8-10 外資合計
    #           11-13 投信 | 14-16 自營(自行) | 17-19 自營(避險) | 20-22 自營合計 | 23 三大合計
    i_fx, i_it, i_dl = 10, 13, 22
    if f:
        a = pick(f, '外資及陸資', '買賣超', exclude=('不含', '外資自營商'))
        b = pick(f, '投信', '買賣超')
        c = pick(f, '自營商', '買賣超', exclude=('自行', '避險', '外資'))
        if None not in (a, b, c):
            i_fx, i_it, i_dl = a, b, c
    out = []
    for row in data:
        if len(row) < 23:
            continue
        out.append([str(row[0]).strip(), str(row[1]).strip(), '上櫃', num(row[i_fx]), num(row[i_it]), num(row[i_dl])])
    return out


def main():
    want = int(sys.argv[1]) if len(sys.argv) > 1 else 60
    os.makedirs(DATA, exist_ok=True)
    no_trade = set(open(HOLI).read().split()) if os.path.exists(HOLI) else set()
    have = {f[:8] for f in os.listdir(DATA) if re.fullmatch(r'\d{8}\.csv', f)}
    now = dt.datetime.now(TW)
    day = now.date() if now.hour >= 17 else now.date() - dt.timedelta(days=1)   # 17:00 後才抓當天
    tried, fails = 0, 0
    trading_seen = 0
    while trading_seen < want and tried < want * 2 + 30:
        tried += 1
        key = day.strftime('%Y%m%d')
        if day.weekday() >= 5 or key in no_trade:
            day -= dt.timedelta(days=1); continue
        if key in have:
            trading_seen += 1; day -= dt.timedelta(days=1); continue
        try:
            a = twse(day)
        except Exception as e:
            print(f'✖ {key} 證交所抓取失敗：{e}'); fails += 1
            if fails >= 3: break
            day -= dt.timedelta(days=1); continue
        time.sleep(SLEEP)
        if a is None:
            print(f'{key} 無資料（休市或尚未公布）')
            if day < now.date():
                no_trade.add(key)
            day -= dt.timedelta(days=1); continue
        try:
            b = tpex(day) or []
        except Exception as e:
            print(f'✖ {key} 櫃買抓取失敗：{e}'); b = []
        time.sleep(SLEEP)
        if not b:
            if (now.date() - day).days <= 3:
                print(f'⚠ {key} 櫃買尚未公布，下次再補'); day -= dt.timedelta(days=1); continue
            print(f'⚠ {key} 櫃買沒資料，只存上市')
        with open(os.path.join(DATA, key + '.csv'), 'w', newline='', encoding='utf-8') as fp:
            w = csv.writer(fp)
            w.writerow(['stock_id', 'name', 'market', 'foreign', 'trust', 'dealer'])
            w.writerows(a + b)
        print(f'{key} 上市 {len(a)} 檔、上櫃 {len(b)} 檔')
        trading_seen += 1
        day -= dt.timedelta(days=1)
    with open(HOLI, 'w') as fp:
        fp.write('\n'.join(sorted(no_trade)))
    n = len([f for f in os.listdir(DATA) if f.endswith('.csv')])
    print(f'目前共有 {n} 個交易日資料')
    if n == 0:
        sys.exit('沒有抓到任何資料，請看上面的錯誤訊息')


if __name__ == '__main__':
    main()
