"""Check one semantically named operator contract per losing query in the fresh TLCS run."""
import re
from collections import Counter
from itertools import groupby
import json

from benchmark_rematch import BENCH, HERE, LEAN, speed_losses
from november26_queries import by_query

BASE = BENCH / "three-db-sf10-20261122"
RUN = BASE / "fresh-tlcs-20261126"
PREVIOUS = BASE / "after-ed-cb-20261126"
MODELS = LEAN / "TinylambOpt/ResearchGoals/QueryRewrites"
MODULES = ("EncodedScanAggregates", "KeyRoutedJoinStates", "PartitionedAggregateStates", "SparseSetAndWindowEvaluation")
LEDGER = HERE / "fresh-tlcs-query-losses.md"


def measurements(run):
    return by_query(speed_losses(run, run / "analytics", run / "cloud_bi/measurements.json"))


def columns(rivals, was_loss):
    one = next(iter(rivals.values()))
    return ["継続" if was_loss else "新規", f"{one.current:.6f}",
            *[f"{rivals[e].target:.6f}" if e in rivals else "—" for e in ("DuckDB", "ClickHouse")]]


def registered_goals():
    names = {}
    for module in MODULES:
        path = MODELS / f"{module}.lean"
        for i, line in enumerate(path.read_text().splitlines(), 1):
            if m := re.match(r"^def (\w+\.Goal) : Prop :=", line):
                assert m[1] not in names
                names[m[1]] = f"../../{path.relative_to(LEAN)}#L{i}"
    return names


def audit():
    latest, previous = measurements(RUN), measurements(PREVIOUS)
    names = registered_goals()
    seen_queries, seen_names = set(), set()
    suite = None
    for line in LEDGER.read_text().splitlines():
        if line.startswith("## "):
            suite = line[3:]
        if not line.startswith("|") or ".Goal](" not in line:
            continue
        query, status, current, duck, click, link, description = [s.strip() for s in line.strip("|").split("|")]
        key = (suite, query)
        assert key not in seen_queries and description
        assert [status, current, duck, click] == columns(latest[key], key in previous), key
        match = re.fullmatch(r"\[(\w+\.Goal)\]\(([^)]+)\)", link)
        assert match and names[match[1]] == match[2] and match[1] not in seen_names, key
        seen_queries.add(key)
        seen_names.add(match[1])
    assert seen_queries == latest.keys() and seen_names == names.keys()
    print(f"{len(seen_queries)} queries, {sum(map(len, latest.values()))} losses, {len(seen_names)} distinct goals")


def sample_url_runs():
    folder = BENCH / "clickbench-full-20261116/dataset"
    names = json.loads((folder / "metadata.json").read_text())["columns"]
    counter, url = (names.index(k) for k in ("CounterID", "URL"))
    path = folder / "hits.tsv"
    stats = Counter()
    with path.open("rb") as stream:
        for w in range(12):
            start = path.stat().st_size*w//12
            stream.seek(max(0, start-1))
            if start and stream.read(1) != b"\n":
                stream.readline()
            rows = []
            for _ in range(8192):
                fields = stream.readline().rstrip(b"\n").split(b"\t")
                rows.append((fields[counter], len(bytes.fromhex(fields[url].decode()))))
            offset = 0
            for _, group in groupby(rows, lambda row: row[0]):
                lengths = [n for _, n in group]
                begin = offset
                empty = 0
                for n in lengths:
                    offset += n
                    empty += n == 0
                assert (offset-begin, len(lengths)-empty) == (sum(n for n in lengths if n), sum(n > 0 for n in lengths))
                stats.update(rows=len(lengths), runs=1, empty=empty)
    print(dict(stats))


def sample_group_boundary():
    import duckdb
    folder = BENCH / "tpcds-sf-20261117/sf10-dataset"
    sql = (folder / "duckdb/q35.sql").read_text()
    joined = sql[sql.index("FROM customer AS"):sql.index("GROUP BY")]
    src, condition = joined.split("  AND EXISTS(", 1)
    keys = ["ca_state", "cd_gender", "cd_marital_status", "cd_dep_count", "cd_dep_employed_count", "cd_dep_college_count"]
    order = ", ".join(k + " NULLS FIRST" for k in keys)
    with duckdb.connect(str(folder / "reference.duckdb"), read_only=True) as db:
        print(db.execute(f"""
            WITH cs AS (
                SELECT c_customer_sk AS customer, dense_rank() OVER (ORDER BY {order}) AS label,
                       EXISTS({condition} AS eligible {src}
            ), sample AS (
                SELECT DISTINCT label FROM cs WHERE customer%64=0 AND eligible ORDER BY label LIMIT 100
            ), boundary AS (SELECT max(label) AS cut, count(*) AS n FROM sample)
            SELECT count(*), count(*) FILTER (WHERE label<=cut), count(*) FILTER (WHERE eligible),
                   count(DISTINCT label) FILTER (WHERE eligible), min(n) FROM cs, boundary
        """).fetchone())


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", action="store_true", help="also replay the two read-only fixture probes (requires DuckDB 2.0.0)")
    args = parser.parse_args()
    audit()
    if args.probe:
        sample_url_runs()
        sample_group_boundary()
