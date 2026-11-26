"""Regenerate the loss-to-goal ledger from the pinned benchmark measurements."""
import argparse
from collections import defaultdict
from dataclasses import dataclass
import json
import math
from pathlib import Path
import re
from statistics import median

HERE = Path(__file__).resolve().parent.parent
LEAN = HERE.parents[1]
BENCH = LEAN.parent / "build/benchmarks"
THREE = BENCH / "three-db-sf10-20261122/after-waves1-13-20261125"
ANALYTICS = BENCH / "analytics-sf10-waves1-13"
CLOUD = BENCH / "cloud-bi-waves1-13/measurements.json"
ENGINES = {"duckdb": "DuckDB", "clickhouse": "ClickHouse"}
THREE_SUITES = {"tpch": ("TPC-H", "Tpch", 22), "tpcds": ("TPC-DS", "Tpcds", 99),
                "clickbench": ("ClickBench", "Click", 43)}
FAMILY = {"job": "JOB", "stats_ceb": "STATS-CEB", "h2o": "H2O",
          "ldbc_snb": "LDBC SNB", "bigbench": "BigBench"}


@dataclass(frozen=True)
class Loss:
    unit: str
    suite: str
    case: str
    rival: str
    current: int | float
    target: int | float
    goal: str


def jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def group_engines(rows, keys):
    groups = defaultdict(dict)
    for r in rows:
        group = groups[tuple(r[k] for k in keys)]
        assert r["engine"] not in group
        group[r["engine"]] = r
    return groups


def compare(unit, suite, case, goal_prefix, ours, rivals):
    assert rivals.keys() == ENGINES.keys()
    for rival, value in rivals.items():
        assert math.isfinite(ours) and math.isfinite(value) and min(ours, value) > 0
        if ours > value:
            name = ENGINES[rival]
            yield Loss(unit, suite, case, name, ours, value, goal_prefix + name)


def speed_losses(three=THREE, analytics=ANALYTICS, cloud=CLOUD):
    for suite, (title, module, count) in THREE_SUITES.items():
        rows = jsonl(three / suite / "results.jsonl")
        assert sorted(r["query"] for r in rows) == list(range(1, count + 1))
        for r in rows:
            q = r["query"]
            if (suite, q) == ("tpcds", 44):
                assert not r["tinylamb"]["verified"]
                continue
            for e in (*ENGINES, "tinylamb"):
                assert r[e]["verified"] and not r[e]["error"] and len(r[e]["samples_ms"]) == 3
                assert median(r[e]["samples_ms"]) == r[e]["median_ms"]
            yield from compare("ms", title, f"Q{q}", f"{module}.Q{q:02d}", r["tinylamb"]["median_ms"],
                               {e: r[e]["median_ms"] for e in ENGINES})

    groups = group_engines(jsonl(analytics / "results.jsonl"), ("family", "query"))
    inventory = json.loads((analytics / "environment.json").read_text())["family_queries"]
    assert groups.keys() == {(s, q) for s, qs in inventory.items() for q in qs}
    for (s, q), engines in groups.items():
        assert set(engines) == {"tinylamb", *ENGINES}
        for r in engines.values():
            assert r["status"] == "ok" and not r["error"] and r["answer_match"]
            assert len(r["samples_ms"]) == 3 and median(r["samples_ms"]) == r["median_ms"]
        yield from compare("ms", FAMILY[s], q, f"Analytics.{q}", engines["tinylamb"]["median_ms"],
                           {e: engines[e]["median_ms"] for e in ENGINES})

    report = json.loads(cloud.read_text())
    groups = group_engines(report["records"], ("suite", "case", "mode"))
    cloud_names = {("Public BI", "MLB Q108"): "Public108", ("Iceberg SQL", "base"): "IceBase"}
    for (suite, case, mode), engines in groups.items():
        if suite == "Iceberg native":
            assert engines.keys() == ENGINES.keys()
            continue
        assert set(engines) == {"tinylamb", *ENGINES}
        for r in engines.values():
            assert not r.get("error") and r["ms"]
            if mode != "mixed":
                assert len(r["ms"]) == report["repeats"]
            if mode == "SELECT":
                assert r["expected"] == engines["tinylamb"]["expected"]
        prefix = cloud_names.get((suite, case), case + mode)
        yield from compare("ms", suite, case, "Cloud." + prefix, median(engines["tinylamb"]["ms"]),
                           {e: median(engines[e]["ms"]) for e in ENGINES})


def published_rows(name):
    rows = {}
    for line in (HERE / name).read_text().splitlines():
        if line.startswith(("| ClickBench |", "| TPC-H SF10 |", "| TPC-DS SF10 |")):
            cells = [s.strip() for s in line.strip("|").split("|")]
            if cells[1].replace(",", "").isdigit():
                assert cells[0] not in rows
                rows[cells[0]] = cells[1:]
    assert rows.keys() == {"ClickBench", "TPC-H SF10", "TPC-DS SF10"}
    return rows


def analytics_size():
    work = ANALYTICS / "work"
    manifest = json.loads((ANALYTICS / "dataset/manifest.json").read_text())
    expected = {t: n for suite in manifest.values() for t, n in suite["tables"].items()}
    root = work / "clickhouse/data/data/analytics_bench"
    assert {d.name for d in root.iterdir()} == expected.keys()
    ch = 0
    for name, n in expected.items():
        parts = []
        for part in (root / name).iterdir():
            if part.name in {"detached", "format_version.txt"}:
                continue
            prefix, lo, hi, _ = part.name.split("_")
            assert prefix == "all"
            parts.append((int(lo), int(hi), part))
        parts.sort()
        assert all(a[1] < b[0] for a, b in zip(parts, parts[1:]))
        assert sum(int((p / "count.txt").read_text()) for _, _, p in parts) == n
        ch += sum(f.stat().st_size for _, _, p in parts for f in p.rglob("*") if f.is_file())
    tl_files = [work / "tinylamb.db.db", work / "tinylamb.db.db.blocks", work / "tinylamb.db.log.meta"]
    segments = list(work.glob("tinylamb.db.log.[0-9]*"))
    assert segments and set(work.glob("tinylamb.db.*")) == set(tl_files + segments)
    assert not (work / "duckdb.duckdb.wal").exists()
    return sum(f.stat().st_size for f in tl_files + segments), {
        "duckdb": (work / "duckdb.duckdb").stat().st_size, "clickhouse": ch}


def storage_losses():
    duck, click = published_rows("compression.md"), published_rows("clickbench-layout.md")
    for title, key in [("TPC-H SF10", "Tpch"), ("TPC-DS SF10", "Tpcds"), ("ClickBench", "Click")]:
        # Row counts, native and prototype totals must agree in both published censuses.
        def number(row, col):
            return int(row[col].replace(",", ""))
        d, c = duck[title], click[title]
        assert all(number(d, i) == number(c, j) for i, j in [(1, 0), (2, 1), (4, 4)])
        rivals = {"duckdb": number(d, 3), "clickhouse": number(c, 2)}
        yield from compare("B", "保存サイズ", title, "Storage." + key, number(d, 2), rivals)
        yield from compare("B", "オフライン試作", title + " 1 MiB", "Storage.Offline" + key, number(d, 4), rivals)
    native, rivals = analytics_size()
    yield from compare("B", "保存サイズ", "派生分析5種合計", "Storage.Analytics", native, rivals)


def probe_frames():
    """Optional in-memory size experiment on the frozen analytics block file."""
    from compression import zstd

    work = ANALYTICS / "work"
    path = work / "tinylamb.db.db.blocks"
    native, _ = analytics_size()
    # 32 B file header, 17 B per frame (offset, length, CRC32, format/width tag).
    plain = adaptive = native - path.stat().st_size + 32
    with path.open("rb") as stream:
        while raw := stream.read(1024 * 1024):
            packed = zstd.compress(raw, level=6)
            assert zstd.decompress(packed) == raw
            best = min(len(raw), len(packed))
            plain += 17 + best
            for width in (4, 8, 16):
                lanes = [raw[lane::width] for lane in range(width)]
                frame = zstd.compress(b"".join(lanes), level=6)
                decoded = zstd.decompress(frame)
                restored = bytearray(len(raw))
                pos = 0
                for lane, data in enumerate(lanes):
                    restored[lane::width] = decoded[pos:pos + len(data)]
                    pos += len(data)
                assert restored == raw and pos == len(decoded)
                best = min(best, len(frame))
            adaptive += 17 + best
    print(f"Complete footprint including catalog and WAL: independent={plain:,} B, adaptive={adaptive:,} B")


def declarations():
    result = {}
    for f in sorted((LEAN / "TinylambOpt/ResearchGoals/BenchmarkRematch").glob("*.lean")):
        src = f.read_text()
        names = re.findall(r"^def (\w+)\.Goal : Prop", src, re.M)
        annotated = re.findall(r"^/-- (.+?) -/\ndef (\w+)\.Goal : Prop", src, re.M)
        assert sorted(names) == sorted(n for _, n in annotated)
        for doc, name in annotated:
            key = f"{f.stem}.{name}"
            assert key not in result
            line = src[:src.index(f"def {name}.Goal")].count("\n") + 1
            result[key] = (doc, f"../../{f.relative_to(LEAN)}#L{line}")
    return result


def render():
    losses = [*speed_losses(), *storage_losses()]
    goals = declarations()
    actual = [r.goal for r in losses]
    assert len(actual) == len(set(actual)), "duplicate matchup"
    assert set(actual) == goals.keys(), f"missing or extra declarations: {set(actual) ^ goals.keys()}"
    lines = ["# 敗北と Lean 命題の対応表", "",
             "11月25日の速度比較と保存サイズの比較に対応する。測定条件と対象外のケースは[調査記録](benchmark-rematch.md)を参照。", ""]
    for suite in dict.fromkeys(r.suite for r in losses):
        rows = [r for r in losses if r.suite == suite]
        size = rows[0].unit == "B"
        unit = "B" if size else "ms"
        lines.extend([f"## {suite}", "", f"| 種目 | 相手 | tinylamb ({unit}) | 相手 ({unit}) | "
                      + ("勝利に必要な削減 (B)" if size else "同点までの短縮 (ms)")
                      + " | 未証明命題と観測量 |", "|---|---|---:|---:|---:|---|"])
        for r in rows:
            doc, link = goals[r.goal]
            # Preserve raw medians. Decimal formatting is just for displaying the gap.
            if size:
                vals = [f"{n:,}" for n in (r.current, r.target, r.current-r.target+1)]
            else:
                vals = [str(r.current), str(r.target), f"{r.current-r.target:.6f}"]
            lines.append(f"| {r.case} | {r.rival} | {' | '.join(vals)} | [{r.goal}]({link})　{doc} |")
        lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true", help="write the ledger; the default checks it")
    parser.add_argument("--probe-analytics", action="store_true", help="also try byte shuffling on all block frames (Python 3.14+)")
    args = parser.parse_args()
    path = HERE / "benchmark-rematch-ledger.md"
    content = render()
    if args.write:
        path.write_text(content)
    elif path.read_text() != content:
        raise SystemExit("Ledger differs from the measurements or declarations. Review and regenerate using --write.")
    print(f"{len(declarations())} distinct loss-to-goal mappings validated")
    if args.probe_analytics:
        probe_frames()
