# 三率三升／營收創新高：推送規格（給 breakout_detector 端實作）

網站：Danny大叔的股市觀測筆記 → 量化研究（/lab）→ 分頁「三率三升／營收新高」
推送：`POST /api/ingest/fundamental`，HMAC 簽章，跟慢牛相同（用 `tools/lab_push.py` 的 `push()` 最簡單）。
同一天重送會覆蓋。`date` 用推送當天（YYYY-MM-DD）。

## 一、要推哪些股票
- 只看 4 碼普通股（含 KY），排除 ETF、特別股、權證。
- 只推「至少符合一項」的股票：三率季增、三率年增、營收創近 12 個月新高（含以上）。
- 建議排序：`rev_high_n` 由大到小，再依 `tri_streak` 由大到小（網站上也可以點欄位重新排序）。

## 二、欄位（columns，照這個順序）

| key | label（表頭） | type | 內容 | desc（表頭提示） | 門檻 |
|---|---|---|---|---|---|
| code | 代號 | code | "2330" | | |
| name | 名稱 | text | 證交所／櫃買簡稱 | | |
| quarter | 財報季 | text | "2026Q2"（最新一季） | 三率用的最新一季財報 | |
| tri_qoq | 三率季增 | check | true／false | 毛利率、營益率、淨利率都比上一季高 | |
| tri_yoy | 三率年增 | check | true／false | 三率都比去年同一季高 | |
| tri_streak | 連升季數 | int | 0、1、2… | 三率季增連續幾季（本季不成立就是 0） | |
| gm | 毛利率% | float | 42.31 | 營業毛利 ÷ 營業收入 × 100（單季） | |
| gm_chg | 毛利率季變 | signed | +1.25 | 本季減上季，單位：百分點 | bad_below: 0 |
| om | 營益率% | float | 30.12 | 營業利益 ÷ 營業收入 × 100（單季） | |
| om_chg | 營益率季變 | signed | +0.80 | 本季減上季，單位：百分點 | bad_below: 0 |
| nm | 淨利率% | float | 25.40 | 稅後淨利 ÷ 營業收入 × 100（單季） | |
| nm_chg | 淨利率季變 | signed | -0.30 | 本季減上季，單位：百分點 | bad_below: 0 |
| rev_month | 營收月份 | month | "2026-09" | 最新一期月營收的月份 | |
| rev | 月營收(億) | float | 2,512.3 | 單月營收，億元，小數 1 位 | |
| rev_yoy | 營收年增 | pct | 18.5 | 與去年同月比，%（傳數字 18.5，不要傳字串） | bad_below: 0 |
| rev_mom | 營收月增 | pct | -3.2 | 與上個月比，% | |
| rev_high | 營收新高 | text | "歷史新高"／"近36月新高"／"近12月新高"／null | 本月營收創新高的程度 | |
| rev_high_n | 創高月數 | int | 120 | 本月營收是近 N 個月最高的 N（歷史新高填有資料的總月數） | |

- 沒有值就給 `null`（網站顯示「—」），不要給空字串或 "N/A"。
- 百分比一律傳數字（18.5 代表 18.5%），網站自動加 % 與紅綠色。
- `bad_below: 0` 的欄位：負值那一格會顯示橘色粗體，提醒「這一項在下滑」。

## 三、計算規則

**三率（單季）**
- 毛利率＝營業毛利 ÷ 營業收入；營業利益率＝營業利益 ÷ 營業收入；稅後淨利率＝本期淨利 ÷ 營業收入（用合併報表，淨利用「本期淨利」總額）。
- 一定要用「單季」數字。財報是累計數：Q2 單季＝上半年累計 − Q1；Q3＝前三季 − 上半年；Q4＝全年 − 前三季。
- 營業收入 ≤ 0 的那一季不計算（三個率都給 null，tri_qoq／tri_yoy 給 false）。

**三率三升**
- 季增（tri_qoq）：本季三個率「都嚴格大於」上一季。
- 年增（tri_yoy）：本季三個率「都嚴格大於」去年同一季。
- 連升季數（tri_streak）：從最新一季往回數，連續幾季都符合季增。
- 財報公布期限：Q1 5/15、Q2 8/14、Q3 11/14、年報 3/31。公布前用上一季，`quarter` 照實填。

**營收創新高**
- 用單月營收（合併營收）。`rev_high_n`＝本月營收大於過去 N−1 個月每一個月的最大 N（只往回比，不含本月）。
- `rev_high` 標籤：N ≥ 有資料的總月數 → "歷史新高"（至少要有 36 個月資料才算歷史）；N ≥ 36 → "近36月新高"；N ≥ 12 → "近12月新高"；N < 12 → null。
- 月營收每月 10 日前公布；還沒公布就用上一期，`rev_month` 照實填。

**資料來源（擇一）**：公開資訊觀測站、FinMind（TaiwanStockFinancialStatements、TaiwanStockMonthRevenue）。

## 四、說明文字（payload 的 help，網站顯示成可收合的「分數怎麼看」）

```
三率是毛利率、營業利益率、稅後淨利率，都用單季財報計算。三率三升（季增）指三個比率都比上一季高；年增指都比去年同一季高。連升季數越多，代表獲利結構改善越持續。

毛利率看產品賺不賺錢，營益率再扣掉營業費用，淨利率是最後真正留下的。三個同時上升，通常代表產品組合變好、規模效益出現，而不只是一次性收入。季變化是百分點，負值會以橘色標示。

營收新高看最新一期月營收在過去幾個月中的位置。歷史新高最強，其次是近 36 個月、近 12 個月新高。搭配營收年增率一起看，年增為負的格子會以橘色標示。

財報與月營收都有公布時間差：月營收每月 10 日前公布，財報約在季後 45 天內公布。表格顯示的是推送當天已公布的最新資料，僅供研究參考，非投資建議。
```

## 五、推送範例（Python）

```python
from lab_push import push   # etf-monitor repo 的 tools/lab_push.py，需環境變數 INGEST_SECRET

COLUMNS = [
    {"key": "code", "label": "代號", "type": "code"},
    {"key": "name", "label": "名稱", "type": "text"},
    {"key": "quarter", "label": "財報季", "type": "text", "desc": "三率用的最新一季財報"},
    {"key": "tri_qoq", "label": "三率季增", "type": "check", "desc": "毛利率、營益率、淨利率都比上一季高"},
    {"key": "tri_yoy", "label": "三率年增", "type": "check", "desc": "三率都比去年同一季高"},
    {"key": "tri_streak", "label": "連升季數", "type": "int", "desc": "三率季增連續幾季"},
    {"key": "gm", "label": "毛利率%", "type": "float", "desc": "營業毛利 ÷ 營業收入（單季）"},
    {"key": "gm_chg", "label": "毛利率季變", "type": "signed", "desc": "本季減上季，百分點", "bad_below": 0},
    {"key": "om", "label": "營益率%", "type": "float", "desc": "營業利益 ÷ 營業收入（單季）"},
    {"key": "om_chg", "label": "營益率季變", "type": "signed", "desc": "本季減上季，百分點", "bad_below": 0},
    {"key": "nm", "label": "淨利率%", "type": "float", "desc": "稅後淨利 ÷ 營業收入（單季）"},
    {"key": "nm_chg", "label": "淨利率季變", "type": "signed", "desc": "本季減上季，百分點", "bad_below": 0},
    {"key": "rev_month", "label": "營收月份", "type": "month", "desc": "最新一期月營收"},
    {"key": "rev", "label": "月營收(億)", "type": "float"},
    {"key": "rev_yoy", "label": "營收年增", "type": "pct", "desc": "與去年同月比", "bad_below": 0},
    {"key": "rev_mom", "label": "營收月增", "type": "pct", "desc": "與上個月比"},
    {"key": "rev_high", "label": "營收新高", "type": "text", "desc": "本月營收創新高的程度"},
    {"key": "rev_high_n", "label": "創高月數", "type": "int", "desc": "本月營收是近 N 個月最高"},
]
HELP = "...（貼上第四節的說明文字）..."

rows = [
    {"code": "2330", "name": "台積電", "quarter": "2026Q2", "tri_qoq": True, "tri_yoy": True, "tri_streak": 3,
     "gm": 59.1, "gm_chg": 0.8, "om": 49.0, "om_chg": 1.1, "nm": 42.5, "nm_chg": 0.6,
     "rev_month": "2026-09", "rev": 3311.2, "rev_yoy": 36.4, "rev_mom": 4.2, "rev_high": "歷史新高", "rev_high_n": 240},
]
push("fundamental", "2026-10-17", rows, columns=COLUMNS, help=HELP,
     note="三率三升／營收創新高｜財報 2026Q2、月營收 2026-09")
```

## 六、網站會怎麼顯示
- `check` 欄：true 顯示 ✓，false 顯示 —。
- `month` 欄："2026-09" 顯示成「9月」（不是今年會加年，如「25年12月」）。
- `pct` 欄：+18.50%，正紅負綠；`signed` 欄：+1.25，正紅負綠。
- 有 `desc` 的表頭會出現 ⓘ，滑鼠移上去看說明；有 `bad_below` 的欄位負值整格橘色粗體。
- 不需要 `_flag`、`legend`（這張表沒有停損狀態）；若要標記也可以照慢牛的規則用。
