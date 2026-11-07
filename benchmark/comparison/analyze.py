#!/usr/bin/env python3
"""Derive auditable Markdown tables and matplotlib figures from a completed run."""

import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import statistics


ENGINES = ("tinylamb", "postgresql", "mysql")
LABELS = ("tinylamb", "PostgreSQL", "MySQL")
COLORS = ("#d67824", "#326690", "#38866a")


def selected_engines(records):
    styles = [('tinylamb', 'tinylamb', '#d67824'),
              ('postgresql', 'PostgreSQL', '#326690'),
              ('mysql', 'MySQL', '#38866a'),
              ('oracle', 'Oracle Free', '#b74738'),
              ('duckdb', 'DuckDB', '#927200')]
    present = {r.get('engine') for r in records}
    return tuple(zip(*(style for style in styles if style[0] in present)))


def load(directory):
    return [json.loads(line) for line in (directory / "results.jsonl").read_text().splitlines()]


def markdown(headers, rows):
    return "\n".join(["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |",
                      *("| " + " | ".join(map(str, row)) + " |" for row in rows)]) + "\n"


def olap_table(records, suite, scale):
    grouped = defaultdict(dict)
    for record in records:
        if record["kind"] == "olap" and record["suite"] == suite and record["rows"] == scale:
            grouped[record["workload"]][record["engine"]] = record
    rows = []
    for name, engines in grouped.items():
        values = []
        for engine in ENGINES:
            record = engines[engine]
            value = f"{record['median_ms']:.3f}" if record["status"] == "ok" else record["status"]
            values.append(value if record["correct"] else f"**不一致／失敗 ({value})**")
        rows.append([name, *values])
    return markdown(["クエリ", *[f"{e} ms" for e in LABELS]], rows), grouped


def oltp_tables(records):
    groups = defaultdict(lambda: defaultdict(list))
    for record in records:
        if record["kind"] == "oltp":
            groups[(record["workload"], record["clients"])][record["engine"]].append(record)
    tps_rows, latency_rows, htap_rows = [], [], []
    for (name, clients), engines in groups.items():
        tps_values, latency_values = [], []
        for engine in ENGINES:
            trials = engines[engine]
            valid = [t for t in trials if t["valid"]]
            if len(valid) != len(trials):
                tps_values.append("不変条件／実行失敗")
                latency_values.append("無効")
                continue
            median = statistics.median(t["tps"] for t in valid)
            low, high = min(t["tps"] for t in valid), max(t["tps"] for t in valid)
            attempts = sum(sum(w["attempted"] for w in t["workers"]) for t in valid)
            errors = sum(t["errors"] for t in valid)
            rate = errors / attempts * 100 if attempts else 0
            tps_values.append(f"{median:,.1f} [{low:,.1f}–{high:,.1f}]; {rate:.2f}%")
            p95 = [t["p95_ms"] for t in valid if t["p95_ms"] is not None]
            p99 = [t["p99_ms"] for t in valid if t["p99_ms"] is not None]
            latency = f"{statistics.median(p95):.3f} / {statistics.median(p99):.3f}" if p95 else "成功サンプルなし"
            sample_count = sum(t["completed"] for t in valid)
            if p95 and (sample_count < 1000 or len(p95) != len(valid)):
                latency += f"（成功{sample_count}件、有値{len(p95)}/{len(valid)}試行）"
            if name == "htap":
                latency = "異種処理混在のため個別の遅延評価には使用しない"
            latency_values.append(latency)
            if name == "htap":
                htap_rows.append([engine, clients,
                                  f"{statistics.median(t['role_tps'].get('transaction', 0) for t in valid):,.1f}",
                                  f"{statistics.median(t['role_tps'].get('reader', 0) for t in valid):,.1f}", f"{rate:.2f}%"])
        tps_rows.append([name, clients, *tps_values])
        latency_rows.append([name, clients, *latency_values])
    return (markdown(["処理", "接続数", *[f"{e} 成功TPS [最小–最大]; 失敗率" for e in LABELS]], tps_rows),
            markdown(["処理", "接続数", *[f"{e} p95 / p99 ms" for e in LABELS]], latency_rows),
            markdown(["製品", "接続数", "書込txn/s", "結合集計query/s", "全要求失敗率"], htap_rows), groups)


def draw(records, output, micro, tpch, oltp):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                         "svg.fonttype": "none", "savefig.bbox": "tight"})

    def save(fig, filename):
        fig.savefig(output / filename)
        fig.savefig(output / filename.replace(".svg", ".png"), dpi=160)

    def horizontal(grouped, filename, title):
        fig, ax = plt.subplots(figsize=(11, max(6, len(grouped) * .40)))
        y = np.arange(len(grouped))
        step = .72 / len(ENGINES)
        for index, (engine, label, color) in enumerate(zip(ENGINES, LABELS, COLORS)):
            offset = (index - (len(ENGINES) - 1) / 2) * step
            values = [engines[engine].get("median_ms", float("nan")) if engines[engine]["correct"] else float("nan") for engines in grouped.values()]
            ax.barh(y + offset, values, height=step * .95, color=color, label=label)
            for position, engines in enumerate(grouped.values()):
                if not engines[engine]["correct"]:
                    ax.text(.99, position + offset, f"{label}: incorrect/failed", transform=ax.get_yaxis_transform(), ha="right", va="center", color="#ab2424", fontsize=8)
        ax.set_yticks(y, grouped.keys())
        ax.invert_yaxis()
        ax.set_xscale("log")
        ax.set_xlabel("End-to-end median latency (ms, log scale; lower is better)")
        ax.set_title(title)
        ax.grid(axis="x", alpha=.2)
        ax.set_axisbelow(True)
        ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1))
        save(fig, filename)
        plt.close(fig)

    largest = max(r["rows"] for r in records if r["kind"] == "olap" and r["suite"] == "micro")
    horizontal(micro, "micro_1m.svg" if largest == 1000000 else f"micro_{largest}.svg",
               f"Portable OLAP microbenchmarks: {largest:,} sales rows")
    if tpch:
        scale = max(r["rows"] for r in records if r["kind"] == "olap" and r["suite"] == "tpch")
        horizontal(tpch, "tpch.svg", f"TPC-H-derived queries, SF {scale}, binary64 (not an audited TPC score)")
    names = ["point_read", "range_read", "update", "hot_update", "transfer", "insert_delete"]
    fig, axes = plt.subplots(2, 3, figsize=(13, 7))
    for ax, name in zip(axes.flat, names):
        for engine, label, color in zip(ENGINES, LABELS, COLORS):
            points = []
            for (workload, clients), engines in oltp.items():
                valid = [t for t in engines[engine] if t["valid"]]
                if workload == name and valid:
                    values = [t["tps"] for t in valid]
                    middle = statistics.median(values)
                    points.append((clients, middle, middle - min(values), max(values) - middle,
                                   any(t["errors"] for t in valid)))
            points.sort()
            if points:
                ax.errorbar([p[0] for p in points], [p[1] for p in points],
                            yerr=[[p[2] for p in points], [p[3] for p in points]], marker="o", capsize=3,
                            color=color, label=label)
                for clients, value, _, _, errors in points:
                    if errors:
                        ax.plot(clients, value, marker="x", color="#ab2424", markersize=9, markeredgewidth=2)
        ax.set_title(name)
        ax.set_xticks(sorted({clients for _, clients in oltp}))
        ax.set_xlabel("Clients")
        ax.set_ylabel("Successful transactions/s")
        ax.grid(alpha=.2)
        ax.set_ylim(bottom=0)
    axes.flat[0].legend()
    fig.suptitle("OLTP scaling: median and min/max of trials; red x = transaction errors observed")
    fig.tight_layout()
    save(fig, "oltp_scaling.svg")
    plt.close(fig)

    selected = ["count", "group_100", "join_group", "left_join"]
    fig, axes = plt.subplots(1, 4, figsize=(14, 3.6))
    for ax, name in zip(axes, selected):
        for engine, label, color in zip(ENGINES, LABELS, COLORS):
            points = sorted((r["rows"], r["median_ms"]) for r in records if r["kind"] == "olap" and r["suite"] == "micro" and r["workload"] == name and r["engine"] == engine and r["correct"])
            ax.plot([p[0] for p in points], [p[1] for p in points], marker="o", color=color, label=label)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_title(name)
        ax.set_xlabel("Sales rows")
        ax.set_ylabel("Latency (ms)")
        ax.grid(alpha=.2)
    axes[0].legend(fontsize=8)
    fig.suptitle("OLAP data scaling (warm medians; log axes)")
    fig.tight_layout()
    save(fig, "olap_scaling.svg")
    plt.close(fig)


def main():
    global ENGINES, LABELS, COLORS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--no-plots", action="store_true")
    args = parser.parse_args()
    records = load(args.directory)
    ENGINES, LABELS, COLORS = selected_engines(records)
    output = args.directory / "analysis"
    output.mkdir(exist_ok=True)
    scales = sorted({r["rows"] for r in records if r["kind"] == "olap" and r["suite"] == "micro"})
    for scale in scales:
        table, micro = olap_table(records, "micro", scale)
        (output / f"micro_{scale}.md").write_text(table)
    tpch_scales = {r["rows"] for r in records if r["kind"] == "olap" and r["suite"] == "tpch"}
    tpch = {}
    if tpch_scales:
        table, tpch = olap_table(records, "tpch", max(tpch_scales))
        (output / "tpch.md").write_text(table)
    tps, latency, htap, oltp = oltp_tables(records)
    (output / "oltp_tps.md").write_text(tps)
    (output / "oltp_latency.md").write_text(latency)
    (output / "htap.md").write_text(htap)
    ratios = {}
    for suite, group in (("micro_largest", micro), ("tpch", tpch)):
        for baseline in ENGINES[1:]:
            pairs = [(engines[baseline]["median_ms"], engines["tinylamb"]["median_ms"])
                     for engines in group.values() if engines[baseline]["correct"] and engines["tinylamb"]["correct"]]
            if pairs:
                ratios[f"{suite}_vs_{baseline}"] = {"n": len(pairs), "tinylamb_faster": sum(a > b for a, b in pairs),
                                                    "geomean_baseline_over_tinylamb": math.exp(statistics.mean(math.log(a / b) for a, b in pairs))}
    (output / "ratios.json").write_text(json.dumps(ratios, indent=2))
    if not args.no_plots:
        draw(records, output, micro, tpch, oltp)
    print(json.dumps(ratios, indent=2))


if __name__ == "__main__":
    main()
