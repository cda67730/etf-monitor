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
| `inst_cobuy/fetch.py` | 證交所／櫃買抓取函式（`twse`、`tpex`）；`inst_cobuy/data/*.csv` 只當首次匯入的種子資料 |

## 現有頁面
`/` 首頁分流（`hub.html`，四張卡片）、`/etf` ETF 日報（原首頁）、`/holdings` 每日持股、`/new-holdings` 新增持股、`/decreased-holdings` 減持、`/cross-holdings` 跨 ETF 重複持股、`/warrant-ranking` 權證排行、`/warrant-volume-comparison` 權證量能、`/inst-cobuy` 三大法人同買、`/admin/etfs` ETF 清單管理＋手動爬取 ETF／權證（首頁原按鈕已移到這裡）、`/login`

## ETF 日報範圍（積極型／不分類）
- `?scope=aggr|all` 切換，記在 cookie `etf_scope`；`etf_scope_middleware` 設 contextvar `_etf_scope`
- `DatabaseQuery.scope_codes()`、`_scope_sql(col)` 把查詢限縮在範圍內的啟用 ETF；`etf_names`／`get_etf_codes()` 也依範圍
- ETF 相關頁面上方有切換鈕（`base.html`）

## 三大法人同買（inst_cobuy）
- 資料來源：證交所 T86（上市）、櫃買中心三大法人買賣明細（上櫃），免費免 token
  - T86 欄位：外陸資買賣超股數（不含外資自營商）＋外資自營商買賣超股數＝外資；投信；自營商買賣超股數（合計）
  - 注意：欄位名稱比對時「不含外資自營商」字樣會誤中排除條件（已修正過一次）
- 定義：張＝股數/1000 四捨五入，≥1 張才算買超；只含 4 碼普通股／KY；連買天數上限 `N=40`；頁面可切換最近 20 個交易日（需 60 個交易日資料）
- 目前：APScheduler 工作 `inst`（平日 18:20，`SCHED_INST`）寫入 PostgreSQL；啟動時背景匯入 `inst_cobuy/data/*.csv`（只補缺的日期）
- 頁面 `/inst-cobuy`、API `/api/inst-cobuy/dates`、`/api/inst-cobuy?date=`、`/api/inst-cobuy.csv?date=`；預設公開，`INST_COBUY_PUBLIC=false` 改為需登入
- 2026-10 已移除 GitHub Actions（inst-cobuy.yml、daily-scraper.yml）與靜態網頁；GitHub Pages 需使用者在 repo Settings 關閉
- 已驗證：2026-09-30 外資、自營與 FinMind 一致；投信 22 檔不同（以官方為準）；資料庫版計算結果與舊靜態版逐欄一致（2026-10-01，1160 檔）

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
6. 市場情緒指標

## 外部依賴：BookReview（使用者電腦上的 n8n + FastAPI，`C:\Users\david\pyrag\BookReview\scripts\etf_report.py`）
每天約 20:07 由 n8n 觸發，**直接解析本站 HTML**，改樣板或路由前務必確認不會壞：
- `POST /login`（form `password`，跟隨轉址）→ 用 cookie session
- `POST /manual-scrape`（需登入；n8n 逾時 100 秒）→ 現在走 `scheduler.runner.run_or_wait`：排程在跑就等、10 分鐘內剛跑完就沿用
- `GET /new-holdings`：解析 `select[name='date'] option` 取最新日期；`/new-holdings?date=&etf_code=` 解析 `tbody tr` 前 6 欄
- `GET /cross-holdings?date=`：解析 `tbody tr[data-stock_code]` 的 data-* 屬性與 `span.badge[title]`
- `GET /holdings?etf_code=00981A&date=&sort_by=shares_desc`：解析 `tbody tr` 前 7 欄（「新股票」「已移除」字樣）
- `GET /api/etf-holdings?etf_code=&date=`（免登入，指定 etf_code 時不受日報範圍影響）
- BookReview 自己有一份寫死的 `KNOWN_ACTIVE_ETFS`，本站新增 ETF 不會自動同步過去

## 其他備註
- 未來可能加 Google AdSense：需公開頁面；github.io 需在根網域放 `ads.txt`，建議用自訂網域。
- `database_config.py`、`diagnose_password_issue.py` 請確認沒有寫死密碼或連線字串（repo 為公開）。
