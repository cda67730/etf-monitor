# ETF 監控系統（etf-monitor）

給接手這個 repo 的人（含 Claude）快速掌握現況與待辦。改架構時請一併更新本檔。

## 部署與執行環境
- **Railway** 自動部署本 repo 的 `main` 分支（push 即部署）。網址：https://etf-monitor-production.up.railway.app/
- 入口：`Dockerfile` → `uvicorn fastapi_app_cloud:app`
- 資料庫：Railway PostgreSQL（`DATABASE_URL`）；連不上時 `database_config.py` 退回 SQLite `etf_holdings.db`
- 登入：單一密碼 `WEB_PASSWORD`，session 存記憶體（重新部署後需重新登入）
- **2026-10 起只有管理功能需要登入**：`/admin/etfs`、`/api/admin/*`、`/manual-scrape*`、`/test-scrape`、`/api/scheduler/*`、`/diagnostic`、`/debug/db-status`；其他瀏覽頁全部公開。登入後依 `next` 參數返回（預設 `/admin/etfs`）
- 排程呼叫用 token：`SCHEDULER_TOKEN`（`/trigger-scrape*` 端點以 `Authorization: Bearer <token>` 驗證；未設時預設值不安全，務必在 Railway 設定）
- ⚠ 每次 push 到 main 都會觸發 Railway 重新部署 → 不要讓排程每天 commit 資料回 repo

## 主要檔案
| 檔案 | 用途 |
|---|---|
| `fastapi_app_cloud.py` | FastAPI 主程式，所有路由、登入、流量限制 |
| `database_config.py` | DB 連線（PostgreSQL / SQLite）、建表 |
| `improved_etf_scraper_cloud.py` | 主動式 ETF 持股爬蟲；`etf_codes` 屬性讀 `etf_registry`。pocket.tw 自 2026-10-01 起需 guest token（`_get_token`／`_api_get`） |
| `etf_registry.py` | ETF 清單（資料表 `etf_registry`）：分類、啟用、排序；`enabled_codes(aggressive_only=)` |
| `scheduler.py` | APScheduler 統一排程與手動執行（`runner.run_async`） |
| `warrant_scraper.py`、`warrant_volume_analyzer.py` | 權證排行與量能分析 |
| `templates/*.html` | Jinja2 樣板，`base.html` 有導覽列（Bootstrap 5） |
| `inst_flow.py` | 三大法人同買／連買：資料表 `inst_daily`、`inst_no_trading`，頁面與 API |
| `mood_fetch.py` | 市場情緒 14 項指標抓取（全部免 API key；FRED 從機房會逾時，不要用） |
| `market_mood.py` | 資料表 `mood_obs`、燈號／賣出條件／自動結論、`/market-mood`、`/api/market-mood` |
| `fut_flow.py` | 期貨籌碼：資料表 `fut_inst`（期交所三大法人各期貨契約，每日每商品每身份一列）、`/futures`、`/api/futures` |
| `inst_cobuy/fetch.py` | 證交所／櫃買抓取函式（`twse`、`tpex`）；`inst_cobuy/data/*.csv` 只當首次匯入的種子資料 |

## 現有頁面
`/` 首頁分流（`hub.html`，四張卡片）、`/etf` ETF 日報（原首頁）、`/holdings` 每日持股、`/new-holdings` 新增持股、`/decreased-holdings` 減持、`/cross-holdings` 跨 ETF 重複持股、`/warrant-ranking` 權證排行、`/warrant-volume-comparison` 權證量能、`/inst-cobuy` 三大法人同買、`/market-mood` 市場情緒指標、`/futures` 期貨籌碼、`/admin/etfs` ETF 清單管理＋手動爬取 ETF／權證（首頁原按鈕已移到這裡）、`/login`
- 導覽列順序（`base.html`）：三大法人同買、ETF日報、市場情緒、期貨籌碼、權證排行、權證量能、ETF 管理，最後是「舊版 ETF」下拉（每日持股、新增持股、減持表、跨ETF重複持股；這些頁 BookReview 仍在解析，不能刪）

## ETF 日報範圍（積極型／不分類）
- `?scope=aggr|all` 切換，記在 cookie `etf_scope`；`etf_scope_middleware` 設 contextvar `_etf_scope`
- `DatabaseQuery.scope_codes()`、`_scope_sql(col)` 把查詢限縮在範圍內的啟用 ETF；`etf_names`／`get_etf_codes()` 也依範圍
- ETF 相關頁面上方有切換鈕（`base.html`）
- ETF 日報的「明細」對話框（單一 ETF 持股）可點欄位排序：股票、權重（預設由大到小）、持股、增減、狀態；手機版用表格上方的排序鈕

## 三大法人同買（inst_cobuy）
- 資料來源：證交所 T86（上市）、櫃買中心三大法人買賣明細（上櫃），免費免 token
  - T86 欄位：外陸資買賣超股數（不含外資自營商）＋外資自營商買賣超股數＝外資；投信；自營商買賣超股數（合計）
  - 注意：欄位名稱比對時「不含外資自營商」字樣會誤中排除條件（已修正過一次）
- 定義：張＝股數/1000 四捨五入，≥1 張才算買超；只含 4 碼普通股／KY；連買天數上限 `N=40`；頁面可切換最近 20 個交易日（需 60 個交易日資料）
- 目前：APScheduler 工作 `inst`（平日 18:20，`SCHED_INST`）寫入 PostgreSQL；啟動時背景匯入 `inst_cobuy/data/*.csv`（只補缺的日期）
- 頁面樣式與 ETF 日報共用 `templates/_ed_css.html`；分頁：三大同買、外資投信同買（`ft_cobuy`／`ft_streak`／`ft_days`／`ft_total`）、土洋對作（`duel`：1＝投信買外資賣、-1＝外資買投信賣，`duel_streak` 同方向連續天數）、外資／投信／自營買超、每日同買；頁面不顯示上市櫃欄位；欄位標題用短字（同買張、連續天、外連、投連…），手機版仍維持表格，只留 股票／同買張／連續天／外資／投信；「今日觀察」是預設收合的抽屜（只露出一句話結論）；預設分頁是「外資投信同買」
- 所有分頁（每日同買除外）都有期間下拉（當日／近 5 日／近 10 日）：`compute()` 另算 `co_d{5,10}`（同買天數）、`co_s{5,10}`（同買日張數合計）、`ft_d*`、`ft_s*`、`f/t/dl{5,10}`（各法人淨買賣合計）、`foreign/trust/dealer_d{5,10}`（期間買超天數）、`duel{5,10}`（以期間合計判斷的對作方向）與 `duel_d{5,10}`；回傳的 rows 含近 10 日內任一法人買超過的股票（約 1,900 檔、1.4MB，靠 GZip 壓縮），各分頁自行過濾；頁面不提供搜尋框
- 全站加 `GZipMiddleware`（BookReview 的 requests 會自動解壓，不影響）
- 頁面 `/inst-cobuy`、API `/api/inst-cobuy/dates`、`/api/inst-cobuy?date=`、`/api/inst-cobuy.csv?date=`；預設公開，`INST_COBUY_PUBLIC=false` 改為需登入
- 2026-10 已移除 GitHub Actions（inst-cobuy.yml、daily-scraper.yml）與靜態網頁；GitHub Pages 需使用者在 repo Settings 關閉
- 已驗證：2026-09-30 外資、自營與 FinMind 一致；投信 22 檔不同（以官方為準）；資料庫版計算結果與舊靜態版逐欄一致（2026-10-01，1160 檔）

## 市場情緒指標（market_mood）
- 改自使用者 Google Drive「總體風險掃描_標準提示詞_v3」；門檻固定沿用提示詞
- 短期：VIX（CBOE CSV）、台股 VIX（期交所月檔 `YYYYMMnew.txt`，只留近 3 個月）、CNN 恐懼貪婪、CBOE 個股賣權買權比、AAII（官方 xls）、微台散戶淨多空 `tmf_retail`（不存 mood_obs：主程式把 `fut_flow.store.retail_series` 註冊到 `market_mood.EXTRA`，`report()` 時合併；散戶淨口數＝−三大法人微台淨額；近一年百分位 ≥80 且淨多＝紅燈「散戶過度偏多」、≥60＝黃燈；溫度計刻度依近一年高低點動態設定）
- 中期：FINRA 保證金負債（官網表格）、保證金佔 GDP（GDP 取自 multpl）、IPO（Renaissance 今年累計）、NYSE 騰落線（WSJ 當日漲跌家數，本站逐日累計，滿 20 日才判斷背離）、美銀牛熊指標（Finvaulta 的 Flow Show 週報內文）
- 長期：巴菲特指標（Yahoo ^W5000 ÷ GDP）、CAPE（multpl）、美債 10Y−2Y（美國財政部 CSV）、LEI（Conference Board 新聞稿文字）
- 已拿掉：NAAIM（2026/7 後停更）、高收益債利差（只有 FRED）、內部人買賣比與 AAII 持股比重（GuruFocus／AAII 擋）、M 平方（Cloudflare）
- 排程 `mood` 週二～六 07:30（`SCHED_MOOD`）；資料庫空的時候啟動即背景抓一次
- 賣出條件 5 項：VIX>25、保證金連 3 月下降、恐懼貪婪由 ≥75 跌破 50、騰落線背離、美銀 >8；≥3 出脫、≥2 警戒升級
- 版面與 ETF 日報共用 `_ed_css.html`：深色行情列一行放 S&P 500、賣出條件 n/5、短中長紅燈數與結論句；貼頂區塊導覽；原本的大型「賣出條件」判讀區塊已拿掉（細節在下方賣出條件表）

## 進行中的重構計畫（2026-10 與使用者確認）
1. ✅ 本檔 CLAUDE.md
2. ✅ **APScheduler 統一排程**（`scheduler.py`；取代 Railway 外部排程與 GitHub Actions）
   - 預設關閉：Railway 設 `SCHEDULER_ENABLED=true` 才啟動；時間用 `SCHED_ETF`（預設 `0 18,19,20 * * 1-5`）、`SCHED_WARRANT`（預設 `40 16 * * 1-5`）覆寫
   - `GET /api/scheduler/status`、`POST /api/scheduler/run/{etf|warrant}`（登入或 Bearer SCHEDULER_TOKEN）
   - 時區 `Asia/Taipei`、設 misfire 寬限、同一工作不重疊
   - 工作：ETF 持股（`scrape_all_etfs` + `scrape_premium_data`）、權證（`scrape_warrants`）、三大法人（平日 18:20）
   - 加 `/api/scheduler/status`；切換完成後才關掉 Railway 原排程（避免重複執行）
   - 待使用者提供：Railway 原本 ETF / 權證排程時間；確認沒有 App Sleeping、單一 replica
3. ✅ **ETF 清單改存資料庫 `etf_registry`**（代號、名稱、分類、啟用、排序）＋管理頁 `/admin/etfs`（需登入）
   - 分類：國內主動、國外為主、高股息（可擴充）
   - 首次部署把寫死的代號匯入，預設「國內主動」，由使用者在管理頁調整
   - 爬蟲改讀啟用中的 ETF；新增代號時先試抓
4. ✅ **三大法人改存 PostgreSQL**：`inst_daily`（trade_date, stock_id, name, market, foreign_net, trust_net, dealer_net）、`inst_no_trading`；頁面 `/inst-cobuy`、API `/api/inst-cobuy`、CSV 下載；首次啟動匯入 `inst_cobuy/data/*.csv`；之後移除 GitHub Actions 與 Pages
   - 環境變數 `INST_COBUY_PUBLIC=true` 時該頁免登入（為了 AdSense）
5. ✅ **首頁分流** `/`（原首頁移到 `/etf`），四張卡片：
   1. 主動式 ETF 日報（積極型）＝排除「國外為主」「高股息」
   2. 主動式 ETF 日報（不分類）＝全部啟用 ETF
   3. 三大法人買賣超及連買
   4. 市場情緒指標（待使用者提供資料）
6. ✅ 市場情緒指標（`/market-mood`，首頁第四張卡片）

## 期貨籌碼（fut_flow）
- 來源：POST `https://www.taifex.com.tw/cht/3/futContractsDateDown`（queryStartDate／queryEndDate `YYYY/MM/DD`、commodityId 空白＝全部），回 CP950 CSV，一次最多查一年；免金鑰，Actions 機房連得到
- 存 `fut_inst`（d, product, inst, net_trade, long_oi, short_oi, net_oi, net_oi_amt）；身份別原文「外資及陸資」「投信」「自營商」
- 頁面 8 張圖（股票期貨、金融期貨、那斯達克100、道瓊、台指期、電子期貨、標普500、費城半導體）＝未平倉多空淨額（口，所有月份合計）；可切外資／投信／自營商，固定顯示近半年（120 個交易日，資料庫保留全部歷史不刪）；每張圖最後一天實心點、前一天空心點，旁邊標「較昨日 ±N」（`dayDiff` 外掛）；手機 2 欄小卡，點卡片放大
- 排程 `fut` 平日 15:10、18:10（`SCHED_FUT`）；資料庫空的時候啟動即背景補兩年（`BACKFILL_DAYS=730`，批次寫入）
- 公開頁，不需登入
- 小台散戶多空比：`fut_oi`（d, product, oi）存小型臺指期貨全市場未平倉（POST `cht/3/futDataDown`，down_type=1、commodity_id=MTX，一般時段各月份未沖銷契約數加總；查詢區間超過約一個月會回 HTML，所以按 28 天分段）；`retail()`＝−(三大法人小台淨額合計)÷全市場未平倉；`update()` 順便更新，`fut_oi` 空的時候啟動即背景補 120 天
- 首頁市場情緒卡片顯示 VIX、CNN 恐懼貪婪、台股 VIX（取自 `market_mood` 的 items）與微台散戶淨多／淨空口數（`retail_net('微型臺指期貨')`＝−三大法人微台淨額合計，附較前日）；小台多空比 `retail()` 保留但首頁不顯示；手機 2×2；恐懼貪婪只顯示現狀文字（極度恐懼／恐懼／中性／貪婪／極度貪婪，依 25／45／55／75 分界，恐懼綠、貪婪紅）＋小字「CNN」，不顯示數值

## 日報 PDF（etf_report.py）
- `GET /report/etf.pdf?date=&scope=aggr|all`（公開；`refresh=1` 強制重寫需登入）、`GET /api/report/etf`（JSON：facts＋ai）；ETF 日報頁右上有 PDF 按鈕
- 流程：程式算「事實清單」→ 接 `templates/report_prompt.txt` 給 Gemini 寫初稿 → **查證**：初稿＋事實清單接 `templates/verify_prompt.txt` 再呼叫 Gemini（開 `google_search` 工具，不指定 JSON 格式、從文字取 JSON），數字對照清單、清單外敘述上網查證，回傳修正稿與 changes；查證兩個模型都失敗就整份改用模板（不採用沒查證的文字）；修改處、搜尋關鍵字、來源記在 `notes`（`/api/report/etf` 可看），model 欄會帶「+查證」，PDF 免責聲明隨之顯示 → `merge_checked` 逐段核對數字，對不上的段落換成 `fallback_text` 模板句子 → `templates/report_pdf.html` 用 WeasyPrint 轉 PDF
- 環境變數：`GEMINI_API_KEY`（沒設＝全部模板句子）、`GEMINI_MODEL`（預設 gemini-3.1-pro-preview）、`GEMINI_FALLBACK_MODEL`（預設 gemini-3.8-flash）；新模型已停用 temperature 等參數，不要加；`GEMINI_VERIFY`（預設 true，設 false 跳過查證，不建議）
- 結果存資料表 `etf_report`（d, scope, fp…）；`fp` 是當日持股筆數＋異動指紋，資料有變才重寫，否則沿用（不重複呼叫 Gemini）
- 外部資料：證交所 MI_INDEX（指數、漲跌家數、個股漲跌）、FMTQIK（成交金額）、BFI82U（三大法人金額）；櫃買上櫃行情用 POST `www/zh-tw/afterTrading/dailyQuotes`（可指定日期）；股票簡稱與產業別抓證交所 ISIN 清單存 `stock_meta`（7 天更新）
- 排程 `report` 平日 20:50（`SCHED_REPORT`）替最新資料日產生積極型與不分類兩份
- Docker 需 Pango 與 `fonts-noto-cjk`（思源黑體）
- 三大法人今日觀察（`inst_report.py`，繼承 `etf_report.Report`）：存同一張 `etf_report` 表（scope='inst'）；提示詞 `templates/inst_observe_prompt.txt`、PDF `templates/inst_report_pdf.html`
  - `GET /report/inst.pdf?date=`（YYYYMMDD 或 YYYY-MM-DD）、`GET /api/inst-cobuy/observe?date=`（只讀已產生的，不觸發 AI；沒有回 404，網頁區塊就隱藏）
  - PDF 三大同買表、外資投信同買表（不含三大同買）都列出前 `LIST_MAX`=45 檔（事實鍵 `_三大同買清單`、`_外資投信同買清單`、`_外資投信同買檔數`，底線開頭不送 Gemini；舊報告缺這鍵時 render 只重算事實補上，不重呼叫 AI）
  - 排程 `inst_report` 平日 18:40、20:40（`SCHED_INST_REPORT`）；指紋含當日三大法人與主動 ETF 異動，資料沒變不重寫
- 按鈕：ETF 日報頁「ETF 報告下載」、三大法人頁「今日報告下載」，旁邊有日期下拉（最近 10 個資料日）；沒登入只能產生最近 `REPORT_MAX_DAYS`（預設 10）個資料日，避免任意日期觸發 AI
- 補產舊日期時，趨勢圖與連續天數只取該日（含）以前的資料

## 外部依賴：BookReview（使用者電腦上的 n8n + FastAPI，`C:\Users\david\pyrag\BookReview\scripts\etf_report.py`）
每天約 20:07 由 n8n 觸發，**直接解析本站 HTML**，改樣板或路由前務必確認不會壞：
- `POST /login`（form `password`，跟隨轉址）→ 用 cookie session
- `POST /manual-scrape`（需登入；n8n 逾時 100 秒）→ 現在走 `scheduler.runner.run_or_wait`：排程在跑就等、10 分鐘內剛跑完就沿用
- `GET /new-holdings`：解析 `select[name='date'] option` 取最新日期；`/new-holdings?date=&etf_code=` 解析 `tbody tr` 前 6 欄
- `GET /cross-holdings?date=`：解析 `tbody tr[data-stock_code]` 的 data-* 屬性與 `span.badge[title]`
- `GET /holdings?etf_code=00981A&date=&sort_by=shares_desc`：解析 `tbody tr` 前 7 欄（「新股票」「已移除」字樣）
- `GET /api/etf-holdings?etf_code=&date=`（免登入，指定 etf_code 時不受日報範圍影響）
- `GET /api/etfs?scope=all|aggr|every`（免登入）：ETF 清單（code、name、short_name、category、enabled）。BookReview 的 `_active_etfs()` 讀這支，失敗時退回它內建的 `KNOWN_ACTIVE_ETFS`

## 其他備註
- 未來可能加 Google AdSense：需公開頁面；github.io 需在根網域放 `ads.txt`，建議用自訂網域。
- `database_config.py`、`diagnose_password_issue.py` 請確認沒有寫死密碼或連線字串（repo 為公開）。
