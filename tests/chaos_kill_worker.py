"""Chaos test: kill a worker mid-run and assert no job is lost.

Run from the host (needs the docker CLI):  python tests\\chaos_kill_worker.py
Stdlib only on purpose, so it needs no pip install.
"""
import json
import re
import subprocess
import sys
import time
import urllib.request

API = "http://localhost:8000"
N_JOBS = 20
JOB_SECONDS = 10
TIMEOUT_S = 150
UUID_RE = r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"


def api(method, path, body=None):
    req = urllib.request.Request(
        API + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def docker(*args) -> str:
    r = subprocess.run(["docker", *args], capture_output=True, text=True)
    return r.stdout + r.stderr          # docker logs can write to either stream


def running_workers() -> list[str]:
    out = docker("ps", "--filter", "name=job-queue-worker", "--format", "{{.Names}}")
    return [n for n in out.split() if n]


def in_flight(container: str, ids: set[str]) -> set[str]:
    """Jobs this worker claimed but has not finished (from its own logs)."""
    logs = docker("logs", "--since", "2m", container)
    claimed = set(re.findall(r"claimed " + UUID_RE, logs)) & ids
    done = set(re.findall(r"done " + UUID_RE, logs))
    return claimed - done


def fail(msg: str):
    print(f"FAIL: {msg}")
    sys.exit(1)


def main():
    workers = running_workers()
    if len(workers) < 2:
        fail(f"need at least 2 running workers, found {workers}")
    print(f"workers: {workers}")

    ids = [
        api("POST", "/jobs", {"type": "sleep", "payload": {"seconds": JOB_SECONDS}})["id"]
        for _ in range(N_JOBS)
    ]
    ids_set = set(ids)
    print(f"submitted {N_JOBS} jobs of {JOB_SECONDS}s")

    # pick the worker that is actually holding jobs, so the kill is mid-job
    victim, held = None, set()
    deadline = time.time() + 15
    while time.time() < deadline and not held:
        time.sleep(1)
        for w in workers:
            f = in_flight(w, ids_set)
            if len(f) > len(held):
                victim, held = w, f
    if not held:
        fail("no worker picked up any job; is the stack running?")

    print(f"killing {victim} while it holds {len(held)} job(s)")
    docker("kill", victim)
    killed_at = time.time()

    jobs = {}
    try:
        while time.time() - killed_at < TIMEOUT_S:
            jobs = {i: api("GET", f"/jobs/{i}") for i in ids}
            counts: dict[str, int] = {}
            for j in jobs.values():
                counts[j["status"]] = counts.get(j["status"], 0) + 1
            print(f"+{time.time() - killed_at:4.0f}s  {counts}")
            if counts.get("succeeded", 0) == N_JOBS:
                break
            time.sleep(5)
    finally:
        docker("start", victim)         # always bring the worker back

    not_done = [i for i, j in jobs.items() if j["status"] != "succeeded"]
    if not_done:
        fail(f"{len(not_done)} job(s) not succeeded (lost?): {not_done}")
    recovered = [i for i, j in jobs.items() if j["attempts"] > 1]
    if not recovered:
        fail("no job was retried, so the kill did not hit a running job")
    if any(j["attempts"] > 2 for j in jobs.values()):
        fail("a job ran more than twice")

    print(f"PASS: all {N_JOBS} jobs succeeded; {len(recovered)} recovered after killing {victim}")


if __name__ == "__main__":
    main()