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
# 吃記憶體的工作（ETF 持股、三大法人、期貨、融資、啟動補資料）另開子程序跑（jobs.py），跑完整個程序結束、記憶體全部還給系統；
# 主程式常駐記憶體不會被撐大（Railway 依記憶體計費）。設 JOBS_SUBPROCESS=false 改回在主程式裡跑
SUBPROC = os.getenv("JOBS_SUBPROCESS", "true").lower() == "true"
_BASE = os.path.dirname(os.path.abspath(__file__))


def subprocess_job(name, fallback=None, timeout=3600, after=None):
    """回傳一個函式：執行時開子程序 `python jobs.py <name>`，等它結束並回傳結果字串；
    子程序的 log 直接輸出到同一個 stdout。after：跑完（不論成敗）在主程式執行，例如清快取。
    SUBPROC 關閉時改呼叫 fallback（同一件事在主程式裡做）。"""
    def f():
        if not SUBPROC and fallback:
            try:
                return fallback()
            finally:
                if after:
                    after()
        import subprocess, sys, tempfile
        fd, path = tempfile.mkstemp(prefix=f"job_{name}_", suffix=".txt")
        os.close(fd)
        try:
            p = subprocess.run([sys.executable, os.path.join(_BASE, "jobs.py"), name, path],
                               cwd=_BASE, timeout=timeout)
            with open(path, encoding="utf-8") as fh:
                res = fh.read().strip()
            if p.returncode != 0:
                raise RuntimeError(res or f"子程序結束碼 {p.returncode}")
            return res
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
            if after:
                try:
                    after()
                except Exception as e:
                    logger.warning(f"[jobs] {name} 跑完清快取失敗：{e}")
    return f


def spawn(name, fallback, timeout=4 * 3600, after=None):
    """啟動時的背景補資料：開一條執行緒跑 subprocess_job（不拖慢啟動）"""
    job = subprocess_job(name, fallback, timeout, after)

    def run():
        t0 = time.time()
        try:
            res = job()
            logger.info(f"[jobs] ✔ {name}（{time.time() - t0:.0f} 秒）：{res}")
        except Exception as e:
            logger.error(f"[jobs] ✖ {name} 失敗：{e}")
    threading.Thread(target=run, daemon=True).start()
MISFIRE = int(os.getenv("SCHED_MISFIRE_SEC", "3600"))


_DOW = ["sun", "mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def cron_trigger(expr):
    """標準 cron（分 時 日 月 週；週 0/7=日、1=一）→ APScheduler CronTrigger。
    APScheduler 3 的數字星期是 0=週一，直接用 from_crontab 會整個錯一天，所以先轉成英文縮寫。
    多個時間不同分鐘時用分號隔開，例如 "0 16 * * 1-5; 20 18 * * 1-5"。"""
    import re
    from apscheduler.triggers.cron import CronTrigger
    parts = [p.strip() for p in expr.split(";") if p.strip()]
    if len(parts) > 1:
        from apscheduler.triggers.combining import OrTrigger
        return OrTrigger([cron_trigger(p) for p in parts])
    m, h, d, mo, w = parts[0].split()
    w = re.sub(r"\d", lambda x: _DOW[int(x.group())], w)
    return CronTrigger(minute=m, hour=h, day=d, month=mo, day_of_week=w, timezone=TZ)


def rss():
    """主程式目前的常駐記憶體（不含子程序），給 log 觀察用"""
    try:
        for ln in open("/proc/self/status"):
            if ln.startswith("VmRSS"):
                return f"主程式記憶體 {int(ln.split()[1]) // 1024} MB"
    except Exception:
        pass
    return "ok"


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

    @staticmethod
    def _trim():
        """工作跑完把用不到的記憶體還給系統（glibc 不會自己還，Railway 依常駐記憶體計費）"""
        import gc, ctypes
        gc.collect()
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception:
            pass

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
            if job_id != "fut_live":          # 每 2 分鐘的小工作不用每次整理
                self._trim()

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


def setup(scraper=None, warrant_scraper=None, extra_jobs=(), etf_after=None):
    """在 FastAPI 啟動時呼叫。extra_jobs: [(job_id, name, func, env_name, default_cron), ...]"""
    global _scheduler
    if scraper:
        def etf_inproc():
            n = scraper.scrape_all_etfs()
            total = len(scraper.etf_codes)
            if not n:
                raise RuntimeError(f"0/{total} 檔成功")
            return f"{n}/{total} 檔成功"
        etf_job = subprocess_job("etf", etf_inproc, timeout=3600, after=etf_after)
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
