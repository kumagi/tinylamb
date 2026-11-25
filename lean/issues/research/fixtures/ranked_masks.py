"""Measure fixed-population bitmaps against dense bits on the same ClickBench rows."""
import argparse
from collections import Counter
from math import comb
from pathlib import Path

import duckdb


def completions(n, k):
    return comb(n, k) if 0 <= k <= n else 0


def encode(bits):
    population = sum(bits)
    k = population
    r = 0
    for i, bit in enumerate(bits):
        if bit:
            r += completions(len(bits) - i - 1, k)
            k -= 1
    width = (comb(len(bits), population) - 1).bit_length()
    return bytes([population]) + r.to_bytes((width + 7) // 8, "little")


def decode(frame, n):
    k = frame[0]
    r = int.from_bytes(frame[1:], "little")
    assert r < comb(n, k)
    bits = []
    for remaining in reversed(range(n)):
        low = completions(remaining, k)
        bit = r >= low
        bits.append(bit)
        if bit:
            r -= low
            k -= 1
    assert k == r == 0
    return bits


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    args = parser.parse_args()
    predicates = {
        "refresh": "IsRefresh <> 0",
        "mobile": "IsMobile <> 0",
        "search": "SearchPhrase <> ''",
        "region_odd": "RegionID % 2 = 1",
    }
    select = ", ".join(f"coalesce(({p}), false)" for p in predicates.values())
    with duckdb.connect(str(args.database), read_only=True) as db:
        rows = db.sql(
            f"SELECT {select} FROM hits WHERE rowid < 65536 ORDER BY rowid"
        ).fetchall()
    assert len(rows) == 65536
    for name, column in zip(predicates, zip(*rows)):
        counts = Counter()
        for i in range(0, len(column), 64):
            bits = list(column[i:i + 64])
            frame = encode(bits)
            assert decode(frame, len(bits)) == bits
            counts.update(rows=len(bits), hits=sum(bits), dense=(len(bits) + 7) // 8,
                          ranked=len(frame), larger_blocks=int(len(frame) > 8))
        print(name, dict(counts))


if __name__ == "__main__":
    main()
