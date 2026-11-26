"""Local CostBench, PBench, Iceberg and Public BI comparisons. See reports/cloud-bi."""
import argparse
import bz2
import concurrent.futures
import contextlib
import csv
import datetime
import hashlib
import json
import os
import platform
import shutil
import socket
import statistics
import subprocess
import tempfile
import threading
import time
import urllib.request
from decimal import Decimal
from pathlib import Path

import analytics_bench as ab

MLB = ("MLB_11.csv.bz2", "https://event.cwi.nl/da/PublicBIbenchmark/MLB/MLB_11.csv.bz2",
       "0d826334e9aee15fb7dac08cb1f9d77df9069baaa64b8a4132007edb8c67e535")
TRACE = ("snowset.csv", "https://raw.githubusercontent.com/ruc-datalab/PBench/"
         "312440f012e46dfcf75ee305c9d28ab8d26912f7/src/Workloads/Snowset/workload1h-5m-30s_1.csv",
         "411cfc015288a7e485001211eb798219380251ab5d00a5b694f06ff920812351")
PUBLISHED_COST = (
    "costbench-run14.json", "https://raw.githubusercontent.com/ClickHouse/CostBench/"
    "0e9bbfecc3eb4073c9aa50bf2555470daaafebc6/full-path-realtime/quotes/snowflake/"
    "results/t2/charts/run14/full_path_cost_performance_clickhouse_vs_snowflake_summary.json",
    "fd471681af332647f9759a5d467f4745fdd49c5557e8136a423c323a877a8129",
)
ICE_REV = "9463c2f6bf643c235dbfd39428948a7f4ae9bcae"
ICE_PREFIX = "data/persistent/equality_deletes/warehouse/mydb/mytable/"
BATCH_SIZE = 128
BATCHES = 12
READERS = 4


def source(cache, spec):
    name, url, digest = spec
    path = cache / name
    if not path.exists():
        with tempfile.NamedTemporaryFile(dir=cache, delete=False) as tmp:
            try:
                with urllib.request.urlopen(url, timeout=90) as response:
                    tmp.write(response.read())
                tmp.flush()
                if hashlib.sha256(Path(tmp.name).read_bytes()).hexdigest() != digest:
                    raise ValueError(f"download checksum mismatch for {name}")
                os.replace(tmp.name, path)
            finally:
                Path(tmp.name).unlink(missing_ok=True)
    if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
        raise ValueError(f"checksum mismatch for {path}")
    return path


def table(name, columns):
    return ab.Table(name, [ab.Column(c.rstrip("?"), kind, c.endswith("?"))
                           for c, kind in columns], 0)


def ints(name, names):
    return table(name, [(col, "I") for col in names.split()])


def mlb_fixture(path):
    cols = ["league", "pos", "yr", "bat_avg", "babip", "iso_val", "woba"]
    schema = table("mlb", [(c, "S") for c in cols[:2]] + [(cols[2], "I")] +
                   [(c + "?", "I") for c in cols[3:]])
    rows = []
    with bz2.open(path, "rt") as src:
        for line in src:
            fields = line.rstrip("\n").split("|")
            stats = [None if fields[i] == "null" else int(Decimal(fields[i]) * 1000)
                     for i in (1, 2, 12, 33)]
            rows.append((fields[28], fields[30], int(fields[37]), *stats))
    cases = []
    for q, i in [(1, 0), (2, 1), (56, 2)]:
        col = cols[i]
        group = col if q != 56 else "yr, yr"
        cases.append((f"MLB Q{q}", f"SELECT {col} FROM mlb GROUP BY {group} ORDER BY {col}",
                      [(v,) for v in sorted({r[i] for r in rows})]))
    group = ("PCL", "Center Fielder", 2015)
    matches = [r for r in rows if r[:3] == group]
    aggs = [op([r[i] for r in matches if r[i] is not None], default=None)
            for i in range(3, 7) for op in (max, min)]
    expr = ", ".join(f"{op}({c})" for c in cols[3:] for op in ("MAX", "MIN"))
    cases.append(("MLB Q108", f"SELECT {expr}, league, pos, yr FROM mlb "
                  "WHERE league='PCL' AND pos='Center Fielder' AND yr=2015 "
                  "GROUP BY league,pos,yr,yr", [(*aggs, *group)] if matches else []))
    expr = "CASE WHEN pos LIKE '% Fielder' THEN 'outfield' ELSE 'other' END"
    counts = {}
    for league, pos, *_ in rows:
        k = (league, "outfield" if pos.endswith(" Fielder") else "other")
        counts[k] = counts.get(k, 0) + 1
    cases.append(("MLB suffix CASE", f"SELECT league, {expr} AS tag, COUNT(*) FROM mlb "
                  f"GROUP BY league, {expr} ORDER BY league, tag",
                  [(*k, v) for k, v in sorted(counts.items())]))
    cases.append(("MLB grouped suffix", f"SELECT league, {expr} AS tag, SUM(n) FROM "
                  "(SELECT league,pos,COUNT(*) AS n FROM mlb GROUP BY league,pos) g "
                  f"GROUP BY league, {expr} ORDER BY league, tag", cases[-1][2]))
    return [(schema, rows)], cases


def pbench_fixture(path):
    with path.open() as src:
        trace = list(csv.DictReader(src))
    facts = [(j % 16, j % 29 - 12) for j in range(256)]
    dims = [(j, j % 3 + 1) for j in range(16)]
    cases = []
    for width in (32, 128, 256):
        leaves = []
        weights = [[0, 0] for _ in dims]
        total = 0
        for i in range(width):
            bucket = i % 16
            fraction = float(trace[i % len(trace)]["join"] or 0)
            join = (i * 37 % 101) / 101 < fraction
            weights[bucket][int(join)] += 1
            measure = "p.v * d.w" if join else "p.v"
            rel = "pb_fact p JOIN pb_dim d ON p.k=d.k" if join else "pb_fact p"
            leaves.append(f"SELECT SUM({measure}) AS s FROM {rel} WHERE p.k={bucket}")
            total += sum(v * (dims[k][1] if join else 1) for k, v in facts if k == bucket)
        groups = ["SELECT SUM(s) AS s FROM (" + " UNION ALL ".join(leaves[i:i+8]) + ") leaf"
                  for i in range(0, width, 8)]
        sql = "SELECT SUM(s) FROM (" + " UNION ALL ".join(groups) + ") branches"
        cases.append((f"{width} branches", sql, [(total,)]))
        if width == 256:
            def multiplicity(joined):
                terms = " ".join(f"WHEN {k} THEN {w[joined]}" for k, w in enumerate(weights))
                return f"(CASE p.k {terms} ELSE 0 END)"
            sql = (f"SELECT SUM(p.v * ({multiplicity(0)} + {multiplicity(1)} * COALESCE(d.sw,0))) "
                   "FROM pb_fact p LEFT JOIN (SELECT k,SUM(w) AS sw FROM pb_dim GROUP BY k) d "
                   "ON p.k=d.k")
            cases.append(("256 factored", sql, [(total,)]))
    return [(ints("pb_fact", "k v"), facts), (ints("pb_dim", "k w"), dims)], cases


def iceberg_fixture():
    # One partition spec; each file has 48 zero-based positions. rid is globally unique.
    files = [(i, i % 6, i % 4 + 1) for i in range(96)]
    rows = [(f * 48 + p, f, p, None if p % 13 == 0 else p % 11)
            for f, _, _ in files for p in range(48)]
    pos = [(f, p, seq) for f, _, seq in files for p in (0, 7, 7, 26)]
    pos += [(f, 13, seq-1) for f, _, seq in files]
    # scope -1 represents an unpartitioned (global) equality delete file.
    eq = [(-1, None, 3), (-1, 7, 2)] + [
        (part, k, seq) for part in (0, 2, 4) for k, seq in ((7, 4), (1, 2), (1, 3))]
    position = ("SELECT x.rid FROM ice_rows x JOIN ice_files f ON x.fid=f.fid "
                "JOIN ice_pos z ON x.fid=z.fid AND x.pos=z.pos WHERE f.seq<=z.seq")
    equality = ("SELECT x.rid FROM ice_rows x JOIN ice_files f ON x.fid=f.fid "
                "JOIN ice_eq e ON COALESCE(x.k,-1)=COALESCE(e.k,-1) "
                "WHERE f.seq<e.seq AND (f.part=e.scope OR e.scope=-1)")
    positions = set(pos)
    cases = []
    for name, use_p, use_e, narrow in (("base", False, False, False), ("position", True, False, False),
                                        ("equality", False, True, False), ("both / part 0", True, True, True)):
        where = ["1=1"]
        if use_p:
            where.append(f"x.rid NOT IN ({position})")
        if use_e:
            where.append(f"x.rid NOT IN ({equality})")
        if narrow:
            where.append("f.part=0")
        live = []
        for rid, fid, p, key in rows:
            _, part, seq = files[fid]
            if narrow and part != 0:
                continue
            if use_p and any((fid, p, s) in positions for s in range(seq, 5)):
                continue
            if use_e and any(scope in (-1, part) and k == key and seq < s for scope, k, s in eq):
                continue
            live.append(rid)
        sql = ("SELECT COUNT(*), SUM(x.rid) FROM ice_rows x JOIN ice_files f ON x.fid=f.fid WHERE "
               + " AND ".join(where))
        cases.append((name, sql, [(len(live), sum(live))]))
    return [(ints("ice_files", "fid part seq"), files), (ints("ice_rows", "rid fid pos k?"), rows),
            (ints("ice_pos", "fid pos seq"), pos), (ints("ice_eq", "scope k? seq"), eq)], cases


def native_fixture(cache):
    # Use the git blob IDs from the pinned tree to verify every data and metadata file.
    root = cache / "native-iceberg"
    base = root / ICE_PREFIX
    url = f"https://api.github.com/repos/duckdb/duckdb-iceberg/git/trees/{ICE_REV}?recursive=1"
    tree_file = root / f"tree-{ICE_REV}.json"
    root.mkdir(parents=True, exist_ok=True)
    if not tree_file.exists():
        with urllib.request.urlopen(url, timeout=60) as response:
            tree = json.load(response)
        tree_file.write_text(json.dumps(tree))
    else:
        tree = json.loads(tree_file.read_text())
    entries = [e for e in tree["tree"] if e["type"] == "blob" and e["path"].startswith(ICE_PREFIX)
               and not e["path"].endswith(".crc")]
    if tree["sha"] != ICE_REV or tree["truncated"] or not entries:
        raise ValueError("incomplete Iceberg fixture tree")

    def fetch(e):
        path = root / e["path"]
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            url = f"https://raw.githubusercontent.com/duckdb/duckdb-iceberg/{ICE_REV}/{e['path']}"
            with urllib.request.urlopen(url, timeout=60) as response:
                path.write_bytes(response.read())
        data = path.read_bytes()
        if hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest() != e["sha"]:
            raise ValueError(f"bad fixture blob {path}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(fetch, entries))
    # The original parquet locations are repo-relative. Supply the paths expected by
    # allow_moved_paths next to the relocated manifests, without changing the fixture.
    nested = base / ICE_PREFIX / "data"
    nested.mkdir(parents=True, exist_ok=True)
    for path in (base / "data").glob("*.parquet"):
        link = nested / path.name
        if not link.exists():
            link.symlink_to(path.resolve())
    return base


def native_cases(engine, base):
    if engine.name == "clickhouse":
        dest = engine.root / "user_files"
        # The local reader resolves the metadata location under the supplied table
        # path, then opens relative manifest/data paths from its user_files disk.
        shutil.copytree(base, dest, dirs_exist_ok=True)
        dest = dest / ICE_PREFIX
        shutil.copytree(base / "metadata", dest / "metadata", dirs_exist_ok=True)
    cases = []
    for v, ids in [(2, [1, 2, 3, 4]), (5, [4]), (7, [4, 5])]:
        if engine.name == "duckdb":
            escaped = str(base).replace("'", "''")
            scan = f"iceberg_scan('{escaped}', version='{v}', allow_moved_paths=true)"
            sql = f"SELECT id FROM {scan} ORDER BY id"
        else:
            metadata = json.loads((base / "metadata" / f"v{v}.metadata.json").read_text())
            sql = (f"SELECT id FROM icebergLocal('{dest.as_posix()}') ORDER BY id "
                   f"SETTINGS iceberg_snapshot_id={metadata['current-snapshot-id']}")
        cases.append((f"native v{v}", sql, [(x,) for x in ids]))
    return cases


def duration(call, *args):
    start = time.perf_counter_ns()
    result = call(*args)
    return (time.perf_counter_ns() - start) / 1e6, result


def validate(actual, expected):
    # PG simple protocol can return INT64 as text. Compare NULL separately.
    def normalize(rows):
        return [tuple(None if v is None else str(v) for v in row) for row in rows]
    if normalize(actual) != normalize(expected):
        raise ValueError(f"result mismatch: got {actual[:3]}, expected {expected[:3]} "
                         f"({len(actual)}/{len(expected)} rows)")


def measure(engine, suite, cases, repeats, records):
    for name, sql, expected in cases:
        modes = ["EXPLAIN", "SELECT"] if suite == "PBench" else ["SELECT"]
        for mode in modes:
            statement = "EXPLAIN " + sql if mode == "EXPLAIN" else sql
            rec = dict(engine=engine.name, suite=suite, case=name, mode=mode, ms=[], sql=statement)
            if mode == "SELECT":
                rec["expected"] = expected
            try:
                for i in range(repeats + 1):
                    elapsed, result = duration(engine.query, statement)
                    if mode == "EXPLAIN":
                        if not result:
                            raise ValueError("empty plan")
                    else:
                        validate(result, expected)
                    if not i and mode == "EXPLAIN":
                        rec["first_ms"] = elapsed
                    if i:
                        rec["ms"].append(elapsed)
            except Exception as exc:
                rec["error"] = str(exc)[:600]
            records.append(rec)
            print(engine.name, suite, name, mode, rec.get("error") or rec["ms"], flush=True)


@contextlib.contextmanager
def session(engine):
    if engine.name == "clickhouse":
        yield engine.query, engine._request
    elif engine.name == "duckdb":
        with contextlib.closing(engine.conn.cursor()) as conn:
            yield lambda sql: conn.execute(sql).fetchall(), conn.execute
    else:
        import psycopg2
        from pg_binary_client import PgExtendedBinaryClient
        with contextlib.closing(psycopg2.connect(**engine.endpoint)) as conn:
            conn.autocommit = True
            dsn_params = conn.get_dsn_parameters()
            with contextlib.closing(
                PgExtendedBinaryClient(
                    host=dsn_params.get("host", "127.0.0.1") or "127.0.0.1",
                    port=int(dsn_params.get("port", 55433)),
                    user=dsn_params.get("user", "bench"),
                    dbname=dsn_params.get("dbname", "bench"),
                )
            ) as bin_client:
                with conn.cursor() as cur:
                    yield bin_client.query, cur.execute


def quotes(batch):
    return [(batch, i % 16, 900 + (i * 13 + batch) % 87) for i in range(BATCH_SIZE)]


def insert_values(name, rows):
    return f"INSERT INTO {name} VALUES " + ",".join(
        "(" + ",".join(str(int(v)) for v in row) + ")" for row in rows)


def mixed(engine):
    engine.create_table(ints("quotes", "batch_id sym bid"))
    engine.create_table(ints("quote_rollup", "batch_id sym n total"))
    samples = {key: [] for key in ("dashboard", "drilldown", "refresh", "ingest")}
    wall = []
    with contextlib.ExitStack() as stack:
        writer, write = stack.enter_context(session(engine))
        readers = [stack.enter_context(session(engine))[0] for _ in range(READERS)]

        def ingest(b):
            write(insert_values("quotes", quotes(b)))

        def refresh(b):
            rows = writer("SELECT sym,COUNT(*),SUM(bid) FROM quotes "
                          f"WHERE batch_id={b} GROUP BY sym")
            write(insert_values("quote_rollup", [(b, *row) for row in rows]))

        ingest(0)
        refresh(0)
        with concurrent.futures.ThreadPoolExecutor(max_workers=READERS) as pool:
            for b in range(1, BATCHES + 1):
                started = []
                barrier = threading.Barrier(READERS + 1, timeout=30,
                                            action=lambda: started.append(time.perf_counter()))
                def read(i):
                    dashboard = i % 2 == 0
                    sym = (b + i) % 16
                    fields = "SUM(n),SUM(total)" if dashboard else "COUNT(*),SUM(bid)"
                    rel = "quote_rollup" if dashboard else "quotes"
                    sql = f"SELECT {fields} FROM {rel} WHERE batch_id<{b} AND sym={sym}"
                    values = [price for j in range(b) for _, k, price in quotes(j) if k == sym]
                    barrier.wait()
                    elapsed, rows = duration(readers[i], sql)
                    validate(rows, [(len(values), sum(values))])
                    return ("dashboard" if dashboard else "drilldown"), elapsed
                jobs = [pool.submit(read, i) for i in range(READERS)]
                barrier.wait()
                samples["ingest"].append(duration(ingest, b)[0])
                samples["refresh"].append(duration(refresh, b)[0])
                for job in jobs:
                    key, elapsed = job.result()
                    samples[key].append(elapsed)
                wall.append((time.perf_counter() - started[0]) * 1000)
        final = [q for j in range(BATCHES + 1) for q in quotes(j)]
        expected = {}
        for batch, symbol, bid in final:
            count, total = expected.get((batch, symbol), (0, 0))
            expected[batch, symbol] = count + 1, total + bid
        validate(writer("SELECT batch_id,sym,n,total FROM quote_rollup ORDER BY batch_id,sym"),
                 [(*key, *values) for key, values in sorted(expected.items())])
    return [dict(engine=engine.name, suite="CostBench", case=k, mode="mixed", ms=vs)
            for k, vs in samples.items()] + [
        dict(engine=engine.name, suite="CostBench", case="mixed round", mode="mixed",
             ms=wall, rows_per_round=BATCH_SIZE, readers=READERS)]


def plot(report, out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(3, 2, figsize=(13, 10), layout="constrained")
    panels = [("CostBench", "mixed", "Quotes · concurrent refresh + four reads"),
              ("Public BI", "SELECT", "Public BI · full MLB_11"),
              ("PBench", "EXPLAIN", "Snowset-derived branches · EXPLAIN + fetch"),
              ("PBench", "SELECT", "Snowset-derived branches · execute + fetch"),
              ("Iceberg SQL", "SELECT", "Iceberg v2 · relational delete application"),
              ("Iceberg native", "SELECT", "Iceberg v2 · native manifest + Parquet scan")]
    colors = {"tinylamb": "#bd4d31", "duckdb": "#176d84", "clickhouse": "#926708"}
    for ax, (suite, mode, title) in zip(axes.flat, panels):
        rows = [r for r in report["records"] if r["suite"] == suite and r["mode"] == mode]
        labels = list(dict.fromkeys(r["case"] for r in rows))
        for j, (engine, color) in enumerate(colors.items()):
            subset = [r for r in rows if r["engine"] == engine and "error" not in r]
            xs = [labels.index(r["case"]) + (j - 1) * .16 for r in subset]
            med = [statistics.median(r["ms"]) for r in subset]
            if subset:
                ax.errorbar(xs, med, yerr=[[m-min(r["ms"]) for m,r in zip(med,subset)],
                                           [max(r["ms"])-m for m,r in zip(med,subset)]],
                            fmt="o", color=color, capsize=2, label=engine)
        ax.set(title=title, ylabel="milliseconds (log scale)", yscale="log")
        ax.set_xticks(range(len(labels)), [s.replace("MLB ", "") for s in labels], rotation=12)
        ax.set_xlim(-.5, max(.5,len(labels)-.5))
        ax.grid(axis="y", alpha=.22)
    handles = [plt.Line2D([], [], color=c, marker="o", linestyle="none", label=k)
               for k, c in colors.items()]
    fig.legend(handles=handles, loc="outside lower center", ncols=3)
    fig.suptitle("Local comparison · median and observed range · missing marks = no valid measurement",
                 fontsize=13)
    fig.savefig(out, dpi=150)
    plt.close(fig)


def plot_published(cost, out):
    import matplotlib.pyplot as plt
    fig, axes = plt.subplots(1, 3, figsize=(9, 3.1), layout="constrained")
    for ax, (key, label) in zip(axes, [("total_cost", "Attributed cost (USD)"),
                                      ("runtime_sec", "Accumulated query time (s)"),
                                      ("score", "Full-path score (USD × s)")]):
        rows = cost["rows"]
        bars = ax.bar([r["label"] for r in rows], [r[key] for r in rows],
                      color=["#926708", "#287eaa"])
        ax.bar_label(bars, labels=[f"{r[key]:,.2f}" for r in rows], padding=2, fontsize=9)
        ax.set_ylim(0, 1.23 * max(r[key] for r in rows))
        ax.set_title(label, fontsize=11)
    fig.suptitle("Published CostBench · matched run14 · enterprise tier")
    fig.savefig(out, dpi=150)
    plt.close(fig)


def run(args):
    args.cache.mkdir(parents=True, exist_ok=True)
    args.output.mkdir(parents=True, exist_ok=True)
    target = args.output / "measurements.json"
    if target.exists():
        raise FileExistsError(target)
    report = dict(started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  platform=platform.platform(), python=platform.python_version(), threads=args.threads,
                  cpu=next((l.split(":",1)[1].strip() for l in Path("/proc/cpuinfo").read_text().splitlines()
                            if l.startswith("model name")), None), repeats=args.repeats, warmup=1,
                  git=subprocess.check_output(["git","rev-parse","HEAD"], text=True).strip(),
                  inputs=[MLB, TRACE, PUBLISHED_COST], iceberg_revision=ICE_REV, versions={}, records=[],
                  mixed_config=dict(batches=BATCHES, batch_size=BATCH_SIZE, readers=READERS),
                  host_load_at_start=os.getloadavg())
    cost = json.loads(source(args.cache, PUBLISHED_COST).read_text())
    report["published_costbench"] = {"formula": cost["formula"], "rows": cost["rows"]}
    cfg = ab.build_parser().parse_args([])
    cfg.server, cfg.clickhouse_binary = args.server, args.clickhouse_binary
    cfg.threads, cfg.read_workers = args.threads, args.threads
    with contextlib.ExitStack() as stack:
        ports = []
        for _ in range(3):
            sock = stack.enter_context(socket.socket())
            sock.bind(("127.0.0.1", 0))
            ports.append(sock.getsockname()[1])
    cfg.tinylamb_port, cfg.clickhouse_http_port, cfg.clickhouse_native_port = ports
    cfg.clickhouse_url = f"http://127.0.0.1:{cfg.clickhouse_http_port}"
    fixtures = {"Public BI": mlb_fixture(source(args.cache, MLB)),
                "PBench": pbench_fixture(source(args.cache, TRACE)), "Iceberg SQL": iceberg_fixture()}
    native = native_fixture(args.cache) if {"duckdb", "clickhouse"}.intersection(args.engines) else None
    with tempfile.TemporaryDirectory(prefix="cloud-bi-") as temp:
        work = Path(temp)
        report["table_rows"] = {schema.name: len(rows) for tables, _ in fixtures.values()
                                for schema, rows in tables}
        for tables, _ in fixtures.values():
            for schema, rows in tables:
                ab.write_csv(work / (schema.name + ".csv"), schema, rows)
        for name in args.engines:
            engine = ab.make_engine(name, cfg, work)
            try:
                engine.available()
                engine.setup()
                report["versions"][name] = engine.version()
                if name == "tinylamb":
                    report["server_sha256"] = hashlib.sha256(args.server.read_bytes()).hexdigest()
                report["records"].extend(mixed(engine))
                for suite, (tables, cases) in fixtures.items():
                    for schema, _ in tables:
                        engine.create_table(schema)
                        engine.load(schema, work / (schema.name + ".csv"))
                    measure(engine, suite, cases, args.repeats, report["records"])
                if name == "duckdb":
                    engine.query("LOAD iceberg")
                if name in ("duckdb", "clickhouse"):
                    measure(engine, "Iceberg native", native_cases(engine, native),
                            args.repeats, report["records"])
            finally:
                engine.close()
                target.write_text(json.dumps(report, indent=2) + "\n")
    plot(report, args.output / "latency.png")
    plot_published(cost, args.output / "published-cost.png")
    return any("error" in r for r in report["records"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, default=Path.home()/".cache"/"tinylamb-cloud-bi")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--engines", nargs="+", choices=ab.ENGINES, default=list(ab.ENGINES))
    parser.add_argument("--server", type=Path, default=Path("build/tinylamb_server"))
    parser.add_argument("--clickhouse-binary", type=Path)
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--plot-only", action="store_true",
                        help="redraw both figures from measurements.json in the output directory")
    args = parser.parse_args()
    if args.plot_only:
        data = json.loads((args.output / "measurements.json").read_text())
        plot(data, args.output / "latency.png")
        plot_published(data["published_costbench"], args.output / "published-cost.png")
        return 0
    if min(args.threads, args.repeats) < 1:
        parser.error("threads and repeats must be positive")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
