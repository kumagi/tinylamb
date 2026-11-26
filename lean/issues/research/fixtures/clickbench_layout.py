"""Measure saved three-engine fixtures and isolate two layout choices (Python 3.14+)."""
import argparse
import itertools
from collections import Counter, defaultdict
from compression import zstd
from datetime import datetime, timezone
from functools import lru_cache
import json
from pathlib import Path
import struct

from tlcs_space import inspect, varint, number

TEXT = ("Title", "URL", "Referer", "OriginalURL")
CLOCKS = ("EventTime", "ClientEventTime", "LocalEventTime")


def part_files(table):
    # This frozen fixture uses non-overlapping, unmutated MergeTree parts.
    parts = []
    for directory in table.iterdir():
        if directory.name in {"detached", "format_version.txt"}:
            continue
        if not directory.is_dir():
            continue
        prefix, low, high, _ = directory.name.split("_")
        assert prefix == "all" and (directory / "count.txt").is_file()
        parts.append((int(low), int(high), directory))
    parts.sort()
    assert all(a[1] < b[0] for a, b in zip(parts, parts[1:]))
    rows = sum(int((p / "count.txt").read_text()) for _, _, p in parts)
    return rows, [f for _, _, p in parts for f in p.rglob("*") if f.is_file()]


def footprints(bench):
    three = bench / "three-db-sf10-20261122"
    hits = bench / "clickbench-full-20261116/server/data/data/cb100m"
    specs = [
        ("ClickBench", hits, "clickbench-tlcs-v3-fixture", 99_997_497, 1),
        ("TPC-H SF10", bench / "tpch-sf10-three-20261120.5gAKz0/server/data/data/tpch",
         "tpch-tlcs-v3-fixture", 86_586_082, 8),
        ("TPC-DS SF10", three / "tpcds-ch-server/data/data/tpcds",
         "tpcds-tlcs-fixture", 191_496_658, 24),
    ]
    for name, root, fixture, expected, table_count in specs:
        tables = [part_files(t) for t in root.iterdir() if t.is_dir()]
        files = list((three / fixture).glob("*.db*"))
        column_files = [f for f in files if ".tlcs." in f.name]
        tl_rows = 0
        for f in column_files:
            with f.open("rb") as src:
                header = src.read(40)
                assert header[:8] == b"TLCS0001"
                tl_rows += struct.unpack_from("<Q", header, 32)[0]
        assert len(tables) == len(column_files) == table_count
        assert tl_rows == sum(n for n, _ in tables) == expected
        print(name, expected, sum(f.stat().st_size for f in files),
              sum(f.stat().st_size for _, fs in tables for f in fs))
    _, files = part_files(hits / "hits")
    names = json.loads((bench / "clickbench-full-20261116/dataset/metadata.json").read_text())["columns"]
    tl = inspect(three / "clickbench-tlcs-v3-fixture/native.db.tlcs.11", names, set())
    top = dict(tl["top"])
    for col in (*TEXT, *CLOCKS):
        # Include size streams and marks, exclude shared part metadata.
        ch = sum(f.stat().st_size for f in files if f.name.startswith(col + "."))
        print(col, sum(top[col].values()), ch)


def blob(value):
    return varint(len(value)) + value


def decode_words(raw, n, bases=None):
    words = []
    pos = 0
    for i in range(n):
        if bases is not None:
            tag = raw[pos]
            pos += 1
            if tag in (0, 1):
                words.append(b"" if tag == 0 else bases[i])
                continue
            assert tag == 2
        length, pos = number(raw, pos)
        words.append(raw[pos:pos + length])
        pos += length
    assert pos == len(raw)
    return words


def pack(raw):
    frame = zstd.compress(raw, level=3)
    assert zstd.decompress(frame) == raw
    return len(frame)


@lru_cache(maxsize=65536)
def seconds(field):
    iso = bytes.fromhex(field.decode()).decode("ascii")
    stamp = int(datetime.fromisoformat(iso).timestamp())
    assert datetime.fromtimestamp(stamp, timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00") == iso
    return stamp


def time_forest(times):
    # Columns are 0..2. Root/parent metadata costs one byte per column.
    price = {}
    for c in range(3):
        for p in (None, *(i for i in range(3) if i != c)):
            deltas = [v[c] if p is None else v[c] - v[p] for v in times]
            raw = struct.pack(f"<{len(times)}q", *deltas)
            price[c, p] = 1 + pack(raw)
    best = None
    for order in itertools.permutations(range(3)):
        parent = {}
        for c in order:
            parent[c] = min([None, *parent], key=lambda p: price[c, p])
        cost = sum(price[c, p] for c, p in parent.items())
        if best is None or cost < best[0]:
            best = (cost, parent)
    # Reconstruct the chosen representation from its transmitted frames.
    decoded = {}
    for c, p in best[1].items():
        vals = [v[c] if p is None else v[c] - v[p] for v in times]
        frame = zstd.compress(struct.pack(f"<{len(vals)}q", *vals), level=3)
        restored = struct.unpack(f"<{len(vals)}q", zstd.decompress(frame))
        decoded[c] = list(restored) if p is None else [a+b for a, b in zip(decoded[p], restored)]
        assert decoded[c] == [t[c] for t in times]
    return best[0]


def sample(dataset, windows=12, n=8192):
    names = json.loads((dataset / "metadata.json").read_text())["columns"]
    selected = (*TEXT, *CLOCKS, "CounterID", "EventDate", "UserID", "WatchID", "URLHash")
    cols = {col: names.index(col) for col in selected}
    tsv = dataset / "hits.tsv"
    totals = Counter()
    with tsv.open("rb") as src:
        for w in range(windows):
            start = tsv.stat().st_size * w // windows
            src.seek(max(0, start - 1))
            if start and src.read(1) != b"\n":
                src.readline()
            keys, values, times = [], [], []
            hashes = defaultdict(Counter)
            packed_dates = [bytearray() for _ in CLOCKS]
            for _ in range(n):
                row = src.readline().rstrip(b"\n").split(b"\t")
                assert len(row) == len(names)
                keys.append((int(row[cols["CounterID"]]), row[cols["EventDate"]],
                             int(row[cols["UserID"]]), bytes.fromhex(row[cols["EventTime"]].decode()),
                             int(row[cols["WatchID"]])))
                times.append(tuple(seconds(row[cols[c]]) for c in CLOCKS))
                for out, c in zip(packed_dates, CLOCKS):
                    value = bytes.fromhex(row[cols[c]].decode())
                    digits = value[:4]+value[5:7]+value[8:10]+value[11:13]+value[14:16]+value[17:19]
                    packed = int(digits).to_bytes(6, "little")
                    assert f"{int.from_bytes(packed, 'little'):014d}".encode() == digits
                    out.extend(packed)
                values.append(tuple(bytes.fromhex(row[cols[c]].decode()) for c in TEXT))
                hashes[values[-1][1]][int(row[cols["URLHash"]])] += 1
            totals["temporal_text_model"] += sum(min(len(v), 10+pack(bytes(v))) for v in packed_dates)
            totals["forest_with_tags"] += time_forest(times)
            totals["url_dict_entries"] += len(hashes)
            totals["hash_exceptions"] += sum(sum(hist.values()) - max(hist.values()) for hist in hashes.values())
            totals["event_base"] += pack(struct.pack(f"<{n}q", *(t[0] for t in times)))
            ordered = sorted(range(n), key=keys.__getitem__)
            totals["moved_ordinals"] += sum(i != k for i, k in enumerate(ordered))
            for lane in (1, 2):
                actual = [t[lane] for t in times]
                residual = [t[lane] - t[0] for t in times]
                totals[f"time{lane}_independent"] += pack(struct.pack(f"<{n}q", *actual))
                binary = struct.pack(f"<{n}q", *residual)
                assert [t[0] + d for t, d in zip(times, struct.unpack(f"<{n}q", binary))] == actual
                totals[f"time{lane}_residual"] += pack(binary)
            for layout, ids in [("source", range(n)), ("key", ordered)]:
                streams = list(zip(*(values[i] for i in ids)))
                for col, seq in zip(TEXT, streams):
                    raw = b"".join(map(blob, seq))
                    assert decode_words(raw, n) == list(seq)
                    totals[layout + "_" + col] += pack(raw)
                    if layout == "source":
                        lens = b"".join(varint(len(x)) for x in seq)
                        chars = b"".join(seq)
                        pos = end = 0
                        for x in seq:
                            size, pos = number(lens, pos)
                            assert chars[end:end + size] == x
                            end += size
                        assert pos == len(lens) and end == len(chars)
                        # One additional uint32 frame offset for the second stream.
                        totals["split_" + col] += pack(lens) + pack(chars) + 4
                bases, original = streams[1], streams[3]
                encoded = b"".join(b"\x00" if not x else b"\x01" if x == base else b"\x02" + blob(x)
                                   for x, base in zip(original, bases))
                assert decode_words(encoded, n, bases) == list(original)
                totals[layout + "_OriginalURL_ref"] += pack(encoded)
            side = struct.pack(f"<{n}H", *ordered)
            restored = [None] * n
            for rid, data in zip(struct.unpack(f"<{n}H", side), (values[i] for i in ordered)):
                restored[rid] = data
            assert restored == values
            totals["key_ordinals"] += pack(side)
            totals["rows"] += n
            totals["empty_original"] += sum(not r[3] for r in values)
            totals["same_nonempty_original"] += sum(bool(r[3]) and r[3] == r[1] for r in values)
    return totals


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("benchmarks", type=Path)
    args = parser.parse_args()
    footprints(args.benchmarks)
    print("zstd", zstd.zstd_version)
    print(json.dumps(sample(args.benchmarks / "clickbench-full-20261116/dataset"), indent=2))
