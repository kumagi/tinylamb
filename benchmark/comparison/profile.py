#!/usr/bin/env python3
"""Native diagnostic profile; use run.py for the three-engine comparison."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import statistics
import subprocess
import tempfile
import time

from run import Client, analyze, load_rows
from workloads import ROOT, accounts, equivalent, olap_queries, sales, schema


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--server", type=Path, default=ROOT / "build/tinylamb_server")
    parser.add_argument("--sizes", nargs="+", type=int, default=[10000, 100000, 1000000])
    parser.add_argument("--memory-mib", type=int, default=64)
    parser.add_argument("--queries", nargs="+", default=["count", "sum", "top_100", "join_group", "left_join"])
    parser.add_argument("--repeats", type=int, default=5)
    args = parser.parse_args()
    if min(args.sizes) < 100 or min(args.memory_mib, args.repeats) < 1:
        parser.error("sizes >=100 and memory/repeats >=1 required")
    args.output.mkdir(parents=True, exist_ok=False)
    cpus = sorted(os.sched_getaffinity(0))[:8]
    environment = {k: v for k, v in os.environ.items() if not k.startswith("TINYLAMB_")}
    environment.update(TINYLAMB_PAGE_POOL_BYTES="536870912",
                       TINYLAMB_QUERY_MEMORY_BYTES=str(args.memory_mib * 1024 * 1024))
    metadata = {"timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "server": str(args.server.resolve()), "server_sha256": hashlib.sha256(args.server.read_bytes()).hexdigest(),
                "cpus": cpus, "memory_mib": args.memory_mib, "load_start": os.getloadavg(),
                "args": vars(args)}
    (args.output / "environment.json").write_text(json.dumps(metadata, default=str, indent=2))
    (args.output / "tracked-changes.patch").write_bytes(subprocess.check_output(["git", "diff", "HEAD"], cwd=ROOT))
    for rows in args.sizes:
        with tempfile.TemporaryDirectory(prefix="tinylamb-profile-") as work, (args.output / f"server-{rows}.log").open("w") as log:
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            process = subprocess.Popen(["taskset", "-c", ",".join(map(str, cpus)), str(args.server.resolve()),
                                        str(Path(work) / "database"), "--port", str(port), "--read-workers", "8"],
                                       env=environment, stdout=log, stderr=log, cwd=ROOT)
            client = None
            try:
                for _ in range(400):
                    try:
                        client = Client("tinylamb", {"tinylamb": port}, 60)
                        break
                    except Exception:
                        if process.poll() is not None:
                            raise RuntimeError("profile server exited during startup")
                        time.sleep(.05)
                if client is None:
                    raise RuntimeError("profile server did not start")
                for ddl in schema():
                    client.execute(ddl)
                load_rows(client, "accounts", accounts(min(rows, 10000)))
                load_rows(client, "sales", sales(rows, min(rows, 10000)))
                analyze(client, ["accounts", "sales"])
                queries = olap_queries(rows)
                for name in args.queries:
                    sql = queries[name]
                    first = client.execute(sql)
                    samples = []
                    stable = True
                    for _ in range(args.repeats):
                        start = time.perf_counter()
                        result = client.execute(sql)
                        samples.append(1000 * (time.perf_counter() - start))
                        stable &= equivalent(first, result)
                    record = {"rows": rows, "workload": name, "sql": sql,
                              "samples_ms": samples, "median_ms": statistics.median(samples),
                              "stable": stable, "result": first,
                              "actual_plan": client.execute("EXPLAIN ANALYZE " + sql)}
                    with (args.output / "results.jsonl").open("a") as stream:
                        stream.write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
                    print(rows, name, record["median_ms"], "stable=", stable, flush=True)
                    if not stable:
                        raise RuntimeError("unstable profile result")
            finally:
                if client:
                    client.close()
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
