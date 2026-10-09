"""In-memory background jobs with live logs.

A long-running matrix computation is started in a background thread so the
HTTP request that starts it can return immediately (hosting proxies kill
requests that take longer than ~30-100s). The browser then polls for log lines.
"""
import logging
import threading
import time
import uuid
from datetime import datetime

JOB_TTL_SECONDS = 30 * 60
MAX_LOG_LINES = 5000

_std_logger = logging.getLogger("jobs")


class Job:
    def __init__(self, job_id, form_values):
        self.id = job_id
        self.form_values = form_values
        self.status = "running"          # running | completed | failed
        self.result = None
        self.error = None
        self.created = time.time()
        self.finished = None
        self._logs = []
        self._lock = threading.Lock()

    # logger-like interface (so it can replace `logging.getLogger()` objects)
    def _emit(self, level, msg, *args):
        try:
            text = msg % args if args else str(msg)
        except Exception:
            text = str(msg)
        _std_logger.log(getattr(logging, level), "[job %s] %s", self.id[:8], text)
        with self._lock:
            if len(self._logs) < MAX_LOG_LINES:
                self._logs.append({
                    "i": len(self._logs),
                    "t": datetime.now().strftime("%H:%M:%S"),
                    "lvl": level,
                    "msg": text,
                })

    def info(self, msg, *args):
        self._emit("INFO", msg, *args)

    def warning(self, msg, *args):
        self._emit("WARNING", msg, *args)

    def error(self, msg, *args):
        self._emit("ERROR", msg, *args)

    def logs_since(self, since):
        with self._lock:
            return list(self._logs[since:]), len(self._logs)


class JobStore:
    def __init__(self):
        self._jobs = {}
        self._lock = threading.Lock()

    def _purge(self):
        now = time.time()
        for jid in [j for j, job in self._jobs.items() if now - job.created > JOB_TTL_SECONDS]:
            del self._jobs[jid]

    def start(self, func, params, form_values):
        """Run func(**params, log=job) in a daemon thread; return the Job."""
        job = Job(str(uuid.uuid4()), form_values)
        with self._lock:
            self._purge()
            self._jobs[job.id] = job

        def runner():
            try:
                job.info("Job started")
                outcome = func(log=job, **params)
                if "error" in outcome:
                    job.error = outcome["error"]
                    job.status = "failed"
                    job.warning("Failed: %s", outcome["error"])
                else:
                    job.result = outcome
                    job.status = "completed"
                    job.info("Done! Opening your matrix...")
            except Exception as e:  # never let the thread die silently
                job.error = str(e)
                job.status = "failed"
                job.warning("Failed: %s", e)
            finally:
                job.finished = time.time()

        threading.Thread(target=runner, daemon=True).start()
        return job

    def get(self, job_id):
        with self._lock:
            return self._jobs.get(job_id)


job_store = JobStore()
