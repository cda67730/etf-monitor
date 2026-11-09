# -*- coding: utf-8 -*-
"""在獨立子程序執行吃記憶體的工作，跑完程序結束、記憶體全部還給系統。

用法：python jobs.py <工作名稱> [結果檔路徑]
  由 scheduler.subprocess_job 呼叫；結果字串寫進結果檔（沒給就印在最後一行），失敗結束碼 1。
  也可以手動跑來測試，例如 `python jobs.py margin`。

工作：etf、inst、inst_seed、fut、fut_intraday、fut_startup、margin、margin_startup
這裡只建資料表物件、不呼叫各模組的 init()（init 會再開背景執行緒／子程序）。
"""
import logging
import sys
import traceback

NAME = sys.argv[1] if len(sys.argv) > 1 else ""
OUT = sys.argv[2] if len(sys.argv) > 2 else None

logging.basicConfig(level=logging.INFO, force=True,
                    format=f"%(asctime)s - job:{NAME} - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("jobs")


def _db():
    # 主程式啟動時已印過建表／連線訊息，子程序每次匯入只顯示重大錯誤（連不上會在下面拋例外）
    mute = logging.Filter()
    mute.filter = lambda r: r.levelno >= logging.CRITICAL
    root = logging.getLogger()
    for h in root.handlers:
        h.addFilter(mute)
    try:
        from database_config import db_config
    finally:
        for h in root.handlers:
            h.removeFilter(mute)
    if not db_config:
        raise RuntimeError("資料庫無法使用")
    return db_config


def job_etf():
    import etf_registry
    import etf_report
    from improved_etf_scraper_cloud import ETFHoldingsScraper
    db = _db()
    etf_registry.init(db)
    market = etf_report.Market(db)
    sc = ETFHoldingsScraper()
    sc.short_names = lambda: {c: (v["short"] or "").replace("*", "").strip()
                              for c, v in market.meta().items() if v.get("short")}
    n = sc.scrape_all_etfs()
    total = len(sc.etf_codes)
    if not n:
        raise RuntimeError(f"0/{total} 檔成功")
    return f"{n}/{total} 檔成功"


def job_inst():
    import inst_flow
    return inst_flow.InstFlow(_db()).scrape()


def job_inst_seed():
    import inst_flow
    return inst_flow.InstFlow(_db()).import_seed()


def job_fut():
    import fut_flow
    return fut_flow.FutStore(_db()).update()


def job_fut_intraday():
    import datetime as dt
    import fut_flow
    return fut_flow.FutStore(_db()).backfill_intraday(dt.datetime.now(fut_flow.TZ).date())


def job_fut_startup():
    import fut_flow
    return fut_flow.startup(fut_flow.FutStore(_db()))


def job_margin():
    import margin_flow
    return margin_flow.MarginStore(_db()).update()


def job_margin_startup():
    import margin_flow
    return margin_flow.startup(margin_flow.MarginStore(_db()))


def main():
    fn = globals().get(f"job_{NAME}")
    if not fn:
        print(f"未知工作：{NAME}", file=sys.stderr)
        return 2
    code, res = 0, ""
    try:
        res = str(fn())
    except Exception as e:
        logger.error(traceback.format_exc())
        code, res = 1, f"{type(e).__name__}: {e}"
    if OUT:
        with open(OUT, "w", encoding="utf-8") as fh:
            fh.write(res[:2000])
    else:
        print(res)
    return code


if __name__ == "__main__":
    sys.exit(main())
