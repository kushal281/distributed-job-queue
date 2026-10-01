# Throughput benchmark: preload N jobs with workers stopped, then time K workers draining them.

import argparse
import json
import subprocess
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

API = "http://localhost:8000"


def call(method, path, body=None):
    req = urllib.request.Request(
        API + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def sh(*args):
    subprocess.run(args, check=True, capture_output=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--jobs", type=int, default=1000)
    ap.add_argument("--seconds", type=float, default=0.2, help="sleep time per job")
    args = ap.parse_args()

    sh("docker", "compose", "stop", "worker")          # let the queue fill
    body = {"type": "sleep", "payload": {"seconds": args.seconds}}

    t = time.time()
    with ThreadPoolExecutor(20) as ex:
        list(ex.map(lambda _: call("POST", "/jobs", body), range(args.jobs)))
    took = time.time() - t
    print(f"submitted {args.jobs} jobs in {took:.1f}s ({args.jobs / took:.0f} submits/s)")

    base = call("GET", "/stats")["jobs"].get("succeeded", 0)
    t0 = time.time()
    sh("docker", "compose", "up", "-d", "--scale", f"worker={args.workers}")
    while call("GET", "/stats")["jobs"].get("succeeded", 0) - base < args.jobs:
        time.sleep(0.5)
    elapsed = time.time() - t0
    print(f"{args.workers} worker(s): {args.jobs} jobs in {elapsed:.1f}s = {args.jobs / elapsed:.1f} jobs/s")


if __name__ == "__main__":
    main()