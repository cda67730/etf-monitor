# -*- coding: utf-8 -*-
"""讀 data/*.csv，計算三大法人同買＋連續買超天數，輸出 site/ 靜態網頁資料與 Excel"""
import glob, json, os, re, shutil
import numpy as np, pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA, SITE = os.path.join(HERE, 'data'), os.path.join(HERE, 'site')
N = 20          # 連買最多回溯天數 / 統計區間
KEEP = 20       # 網頁可切換的日期數
W = {'foreign': '外資', 'trust': '投信', 'dealer': '自營商'}


def streak(v):
    n = 0
    for x in v[::-1]:
        if x > 0: n += 1
        else: break
    return n


def load():
    fr = []
    for f in sorted(glob.glob(os.path.join(DATA, '*.csv'))):
        d = pd.read_csv(f, dtype={'stock_id': str})
        d['date'] = os.path.basename(f)[:8]
        fr.append(d)
    df = pd.concat(fr)
    df = df[df.stock_id.str.fullmatch(r'[1-9]\d{3}')]          # 4 碼普通股 / KY
    for k in W: df[k] = (df[k] / 1000).round(0)                 # 張，零股不計
    return df


def as_of(df, dates, last):
    win = [d for d in dates if d <= last][-N:]
    sub = df[df.date.isin(win)]
    names = sub.sort_values('date').groupby('stock_id')[['name', 'market']].last()
    res = pd.DataFrame(index=names.index)
    res['name'], res['market'] = names.name, names.market
    G = {}
    for k in W:
        g = sub.pivot_table(index='stock_id', columns='date', values=k, aggfunc='sum').reindex(index=res.index, columns=win)
        G[k] = g
        res[k] = g[last]
        res[k + '_streak'] = [streak(r) for r in np.nan_to_num(g.values, nan=0)]
        res[k + '_sum'] = g.sum(axis=1)
        res[k + '_days'] = (g > 0).sum(axis=1)
    allb = (G['foreign'] > 0) & (G['trust'] > 0) & (G['dealer'] > 0)
    res['co_streak'] = [streak(r.astype(int)) for r in allb.values]
    res['co_days'] = allb.sum(axis=1)
    res['total'] = res[list(W)].sum(axis=1)
    res['cobuy'] = allb[last]
    res = res[res[list(W)].notna().any(axis=1)]
    return res, win


def to_records(res):
    keep = res[(res[list(W)] > 0).any(axis=1)].copy()          # 至少一個法人買超
    keep = keep.reset_index().rename(columns={'stock_id': 'id'})
    keep = keep.replace({np.nan: None})
    for c in keep.columns:
        if c not in ('id', 'name', 'market', 'cobuy'):
            keep[c] = keep[c].apply(lambda v: None if v is None else int(v))
    keep['cobuy'] = keep['cobuy'].astype(bool)
    return keep.to_dict(orient='records')


def excel(res, last, path):
    t = res.rename(columns={'name': '名稱', 'market': '市場'})
    cols = ['名稱', '市場']
    for k, z in W.items():
        t = t.rename(columns={k: f'{z}買超(張)', k + '_streak': f'{z}連買天數', k + '_sum': f'{z}20日累計(張)', k + '_days': f'{z}20日買超天數'})
    t = t.rename(columns={'co_streak': '三大同買連續天數', 'co_days': '20日內同買天數', 'total': '三大合計(張)'})
    t.index.name = '代號'
    co = t[res.cobuy].sort_values('三大合計(張)', ascending=False)[cols + [c for z in W.values() for c in (f'{z}買超(張)', f'{z}連買天數')] + ['三大合計(張)', '三大同買連續天數', '20日內同買天數']]
    with pd.ExcelWriter(path) as xw:
        co.to_excel(xw, sheet_name='今日三大同買')
        for z in W.values():
            r = t[t[f'{z}買超(張)'] > 0].sort_values(f'{z}買超(張)', ascending=False).head(100)
            r[cols + [f'{z}買超(張)', f'{z}連買天數', f'{z}20日買超天數', f'{z}20日累計(張)', '三大合計(張)']].to_excel(xw, sheet_name=f'{z}買超排行')


def main():
    df = load()
    dates = sorted(df.date.unique())
    if len(dates) < N:
        print(f'⚠ 只有 {len(dates)} 個交易日資料，連買天數上限會小於 {N}')
    out = os.path.join(SITE, 'data'); os.makedirs(out, exist_ok=True)
    idx, cobuy_hist = [], []
    for last in dates[-KEEP:]:
        res, win = as_of(df, dates, last)
        recs = to_records(res)
        json.dump({'date': last, 'window': [win[0], win[-1]], 'n_days': len(win), 'rows': recs},
                  open(os.path.join(out, f'{last}.json'), 'w', encoding='utf-8'), ensure_ascii=False, separators=(',', ':'))
        co = res[res.cobuy].sort_values('total', ascending=False)
        cobuy_hist.append({'date': last, 'count': int(len(co)),
                           'top': [f'{i} {n}' for i, n in zip(co.index[:15], co.name[:15])]})
        idx.append(last)
    json.dump({'dates': idx[::-1], 'cobuy_history': cobuy_hist[::-1]},
              open(os.path.join(out, 'index.json'), 'w', encoding='utf-8'), ensure_ascii=False)
    shutil.copy(os.path.join(HERE, 'web', 'index.html'), os.path.join(SITE, 'index.html'))
    res, _ = as_of(df, dates, dates[-1])
    excel(res, dates[-1], os.path.join(SITE, f'inst_cobuy_latest.xlsx'))
    co = res[res.cobuy].sort_values('total', ascending=False)
    print(f'最新交易日 {dates[-1]}，三大同買 {len(co)} 檔')
    print(co.head(10)[['name', 'foreign', 'trust', 'dealer', 'total', 'co_streak']].to_string())


if __name__ == '__main__':
    main()
