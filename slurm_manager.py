import os
import time
import json
import uuid
import flufl.lock
import collections

Job = collections.namedtuple(
    "Job",
    ["name", "args", "log", "output", "error", "status", "tries", "max_tries", "time"],
    defaults=[None, None, None, None, None, 0, 0, 0],
)


class SlurmManager:
    def __init__(self, job_list_path, max_retries=5, processing_timeout_hours=10):
        self.max_retries = max_retries
        self.processing_timeout_ns = int(processing_timeout_hours * 3600 * 1e9)

        basename = os.path.basename(job_list_path)
        base_dir = os.path.dirname(job_list_path)

        self.job_list_path = job_list_path
        self.lock_path = os.path.join(base_dir, f"{basename}.lock")

    def add_jobs(self, jobs, lock=None):
        do_lock = lock is None
        if do_lock:
            lock = self.new_lock()
            lock.lock()

        f = None
        fname = None
        try:
            if not os.path.exists(self.job_list_path):
                open(self.job_list_path, "w").close()
            lines = open(self.job_list_path, "r").readlines()

            fname = str(uuid.uuid4())
            f = open(fname, "w")
            for line in lines:
                if line.startswith("#"):
                    f.write(line)
                    continue
                if line.startswith("{"):
                    line_data = json.loads(line)
                    job = Job(**line_data)
                else:
                    raise ValueError("Invalid line in job list")
                f.write(json.dumps(job._asdict()) + "\n")

            for job in jobs:
                if type(job) == dict:
                    job = Job(**job)
                job = job._replace(
                    status="unprocessed",
                    tries=0,
                    max_tries=self.max_retries,
                    time=time.time_ns(),
                )
                f.write(json.dumps(job._asdict()) + "\n")
            f.close()
            os.rename(fname, self.job_list_path)
        except Exception as e:
            raise e
        finally:
            if f is not None:
                f.close()
                if os.path.exists(fname):
                    os.remove(fname)
            if do_lock:
                lock.unlock()

    def add_job(self, job_name, args, lock=None):
        do_lock = lock is None
        if do_lock:
            lock = self.new_lock()
            lock.lock()

        f = None
        fname = None
        try:
            if not os.path.exists(self.job_list_path):
                open(self.job_list_path, "w").close()
            lines = open(self.job_list_path, "r").readlines()

            fname = str(uuid.uuid4())
            f = open(fname, "w")
            for line in lines:
                if line.startswith("#"):
                    f.write(line)
                    continue
                if line.startswith("{"):
                    line_data = json.loads(line)
                    job = Job(**line_data)
                else:
                    raise ValueError("Invalid line in job list")
                f.write(json.dumps(job._asdict()) + "\n")

            job = Job(
                name=job_name,
                args=args,
                status="unprocessed",
                tries=0,
                max_tries=self.max_retries,
                time=time.time_ns(),
            )
            f.write(json.dumps(job._asdict()) + "\n")
            f.close()
            os.rename(fname, self.job_list_path)
        except Exception as e:
            raise e
        finally:
            if f is not None:
                f.close()
                if os.path.exists(fname):
                    os.remove(fname)
            if do_lock:
                lock.unlock()

    def new_lock(self, lock_path=None, duration=120):
        if lock_path is None:
            lock_path = self.lock_path
        return flufl.lock.Lock(lock_path, duration)

    def read_line(self, s):
        if s.startswith("#"):
            return None
        elif s.startswith("{"):
            return json.loads(s)
        else:
            raise ValueError("Invalid line in job list")

    def set_status(self, job_name, status, lock=None):
        do_lock = lock is None
        if do_lock:
            lock = self.new_lock()
            lock.lock()

        f = None
        fname = None
        try:
            if not os.path.exists(self.job_list_path):
                open(self.job_list_path, "w").close()
            lines = open(self.job_list_path, "r").readlines()

            status_set = False

            fname = str(uuid.uuid4())
            f = open(fname, "w")
            for line in lines:
                if line.startswith("#"):
                    f.write(line)
                    continue
                if line.startswith("{"):
                    line_data = json.loads(line)
                    job = Job(**line_data)
                else:
                    raise ValueError("Invalid line in job list")
                if job.name == job_name:
                    status_set = True
                    if type(status) == str:
                        job = job._replace(status=status)
                    else:
                        job = job._replace(**status)
                    job = job._replace(time=time.time_ns())
                    f.write(json.dumps(job._asdict()) + "\n")
                else:
                    f.write(line)

            if not status_set:
                if type(status) == str:
                    raise ValueError(
                        "Job name not found, and status is not a dictionary"
                    )

                job = Job(name=job_name, **status)
                job = job._replace(time=time.time_ns())
                f.write(json.dumps(job._asdict()) + "\n")
            f.close()
            os.rename(fname, self.job_list_path)
        except Exception as e:
            raise e
        finally:
            if f is not None:
                f.close()
                if os.path.exists(fname):
                    os.remove(fname)
            if do_lock:
                lock.unlock()

    def acquire_next_job(self, lock=None):
        do_lock = lock is None
        if do_lock:
            lock = self.new_lock()
            lock.lock()

        try:
            if not os.path.exists(self.job_list_path):
                open(self.job_list_path, "w").close()
            lines = [
                s
                for s in open(self.job_list_path, "r").readlines()
                if not s.startswith("#")
            ]

            next_job = None
            for line in lines:
                if line.startswith("{"):
                    line_data = json.loads(line)
                    job = Job(**line_data)
                else:
                    raise ValueError("Invalid line in job list")
                status = job.status
                status = status.lower()
                tries = job.tries
                max_tries = job.max_tries
                job_time = job.time
                can_process = (
                    status == "written"
                    or status == "unprocessed"
                    or (status == "failed" and tries < max_tries)
                    or (
                        status == "processing"
                        and job_time < time.time_ns() - self.processing_timeout_ns
                    )
                )

                if can_process:
                    next_job = job
                    self.set_status(
                        next_job.name,
                        {
                            "status": "processing",
                            "tries": tries + 1,
                            "max_tries": max_tries,
                        },
                        lock,
                    )
                    break
        except Exception as e:
            raise e
        finally:
            if do_lock:
                lock.unlock()

        return next_job

    def mark_unprocessed(self, job_name, lock=None):
        self.set_status(job_name, "unprocessed", lock)

    def mark_processing(self, job_name, lock=None):
        self.set_status(job_name, "processing", lock)

    def mark_processed(self, job_name, lock=None):
        self.set_status(job_name, "processed", lock)

    def mark_failure(self, job_name, lock=None):
        self.set_status(job_name, "failed", lock)
