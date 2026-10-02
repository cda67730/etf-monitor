# -*- coding: utf-8 -*-
"""APScheduler 統一排程（取代 Railway 外部排程與 GitHub Actions）

環境變數（Railway Variables）
  SCHEDULER_ENABLED   true 才會啟動排程（預設 false，確認後再打開，避免與舊排程重複執行）
  SCHED_ETF           ETF 持股＋折溢價，cron 格式（台灣時間），預設 "0 18,19,20 * * 1-5"
  SCHED_WARRANT       權證排行，預設 "40 16 * * 1-5"
  SCHED_INST          三大法人買賣超，預設 "20 18 * * 1-5"
  SCHED_MOOD          市場情緒指標，預設 "30 7 * * 2-6"（美股收盤後）
  SCHED_MISFIRE_SEC   錯過排程的補跑寬限秒數，預設 3600（重新部署時仍會補跑）

狀態查詢：GET /api/scheduler/status
"""
import logging
import os
import threading
import time
import traceback
from datetime import datetime
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

TZ = "Asia/Taipei"
_TZ = ZoneInfo(TZ)


def _now():
    return datetime.now(_TZ).isoformat(timespec="seconds")
ENABLED = os.getenv("SCHEDULER_ENABLED", "false").lower() == "true"
MISFIRE = int(os.getenv("SCHED_MISFIRE_SEC", "3600"))


_DOW = ["sun", "mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def cron_trigger(expr):
    """標準 cron（分 時 日 月 週；週 0/7=日、1=一）→ APScheduler CronTrigger。
    APScheduler 3 的數字星期是 0=週一，直接用 from_crontab 會整個錯一天，所以先轉成英文縮寫。"""
    import re
    from apscheduler.triggers.cron import CronTrigger
    m, h, d, mo, w = expr.split()
    w = re.sub(r"\d", lambda x: _DOW[int(x.group())], w)
    return CronTrigger(minute=m, hour=h, day=d, month=mo, day_of_week=w, timezone=TZ)


class JobRunner:
    """記錄每個工作的最近執行結果，並確保同一工作不會同時跑兩次"""

    def __init__(self):
        self.jobs = {}          # job_id -> dict(name, func, cron)
        self.state = {}         # job_id -> dict(last_start, last_end, last_status, last_message, running)
        self.locks = {}

    def add(self, job_id, name, func, cron):
        self.jobs[job_id] = {"name": name, "func": func, "cron": cron}
        self.state[job_id] = {"running": False, "last_start": None, "last_end": None,
                              "last_status": None, "last_message": None, "last_seconds": None}
        self.locks[job_id] = threading.Lock()

    def run(self, job_id):
        lock = self.locks[job_id]
        st = self.state[job_id]
        if not lock.acquire(blocking=False):
            logger.warning(f"[scheduler] {job_id} 仍在執行，略過本次")
            return
        t0 = time.time()
        st.update(running=True, last_start=_now())
        logger.info(f"[scheduler] ▶ 開始 {self.jobs[job_id]['name']}")
        try:
            result = self.jobs[job_id]["func"]()
            st.update(last_status="success", last_message=str(result)[:300])
            logger.info(f"[scheduler] ✔ 完成 {job_id}：{result}")
        except Exception as e:
            st.update(last_status="error", last_message=f"{e}")
            logger.error(f"[scheduler] ✖ {job_id} 失敗：{e}\n{traceback.format_exc()}")
        finally:
            st.update(running=False, last_end=_now(),
                      last_seconds=round(time.time() - t0, 1))
            lock.release()

    def run_or_wait(self, job_id, fresh_sec=600, timeout=1200):
        """給外部同步呼叫（例如 BookReview 的 /manual-scrape）：
        - 正在跑：等它跑完，回傳那次結果，不重複執行
        - fresh_sec 秒內剛成功跑完：直接回傳上次結果
        - 否則：立即執行一次並回傳結果"""
        st = self.state[job_id]
        lock = self.locks[job_id]
        if st["running"] or lock.locked():
            if lock.acquire(timeout=timeout):
                lock.release()
            return dict(st, reused=True)
        if st["last_status"] == "success" and st["last_end"]:
            age = (datetime.now(_TZ) - datetime.fromisoformat(st["last_end"])).total_seconds()
            if age < fresh_sec:
                return dict(st, reused=True)
        self.run(job_id)
        return dict(st, reused=False)

    def run_async(self, job_id):
        threading.Thread(target=self.run, args=(job_id,), daemon=True).start()


runner = JobRunner()
_scheduler = None


def setup(scraper=None, warrant_scraper=None, extra_jobs=()):
    """在 FastAPI 啟動時呼叫。extra_jobs: [(job_id, name, func, env_name, default_cron), ...]"""
    global _scheduler
    if scraper:
        def etf_job():
            n = scraper.scrape_all_etfs()
            total = len(scraper.etf_codes)
            if not n:
                raise RuntimeError(f"0/{total} 檔成功")
            return f"{n}/{total} 檔成功"
        runner.add("etf", "ETF 持股＋折溢價", etf_job,
                   os.getenv("SCHED_ETF", "0 18,19,20 * * 1-5"))
    if warrant_scraper:
        runner.add("warrant", "權證排行", lambda: warrant_scraper.scrape_warrants(pages=5, sort_type=3),
                   os.getenv("SCHED_WARRANT", "40 16 * * 1-5"))
    for job_id, name, func, env_name, default_cron in extra_jobs:
        runner.add(job_id, name, func, os.getenv(env_name, default_cron))

    if not ENABLED:
        logger.info("[scheduler] SCHEDULER_ENABLED 未開啟，排程不啟動（可手動用 /api/scheduler/run/<job>）")
        return None

    from apscheduler.schedulers.background import BackgroundScheduler

    _scheduler = BackgroundScheduler(timezone=TZ, job_defaults={
        "coalesce": True, "max_instances": 1, "misfire_grace_time": MISFIRE})
    for job_id, j in runner.jobs.items():
        _scheduler.add_job(runner.run, cron_trigger(j["cron"]),
                           args=[job_id], id=job_id, name=j["name"], replace_existing=True)
    _scheduler.start()
    for job in _scheduler.get_jobs():
        logger.info(f"[scheduler] 已排程 {job.id}（{job.name}）下次 {job.next_run_time}")
    return _scheduler


def shutdown():
    if _scheduler:
        _scheduler.shutdown(wait=False)


def status():
    nxt = {}
    if _scheduler:
        for job in _scheduler.get_jobs():
            nxt[job.id] = job.next_run_time.isoformat() if job.next_run_time else None
    return {
        "enabled": ENABLED,
        "running": bool(_scheduler and _scheduler.running),
        "timezone": TZ,
        "now": _now(),
        "jobs": {jid: {"name": j["name"], "cron": j["cron"], "next_run": nxt.get(jid), **runner.state[jid]}
                 for jid, j in runner.jobs.items()},
    }
