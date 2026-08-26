#!/usr/bin/env python3
"""Refresh query/testdata/googlesql_compliance from google/googlesql.

Omits differential-privacy / anonymization files. Origin is Apache-2.0.
"""

from __future__ import annotations

import json
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HEADERS = {"User-Agent": "tinylamb-compliance-fetch"}

# Keep this in sync with TINYLAMB_GOOGLESQL_VERSION in CMakeLists.txt.
REF = "2026.7.2"
REPO = (
    "https://api.github.com/repos/google/googlesql/contents/"
    f"googlesql/compliance/testdata?ref={REF}"
)
RAW = (
    f"https://raw.githubusercontent.com/google/googlesql/{REF}/"
    "googlesql/compliance/testdata/"
)
SKIP = (
    "differential_privacy",
    "anonymization",
    "aggregation_threshold",
    "pipe_aggregate_with_dp",
    # Graph (GQL) queries are out of scope for tinylamb.
    "graph_",
)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    out = root / "query" / "testdata" / "googlesql_compliance"
    out.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(
        REPO, headers=HEADERS
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        listing = json.load(response)
    names = []
    for item in listing:
        if item.get("type") != "file":
            continue
        name = item["name"]
        if not name.endswith(".test"):
            continue
        low = name.lower()
        if any(token in low for token in SKIP):
            continue
        names.append(name)

    wanted = set(names)
    for existing in out.glob("*.test"):
        if existing.name not in wanted:
            existing.unlink()

    def fetch(name: str) -> tuple[str, str | None]:
        dest = out / name
        try:
            request = urllib.request.Request(RAW + name, headers=HEADERS)
            with urllib.request.urlopen(request, timeout=60) as response:
                dest.write_bytes(response.read())
            return name, None
        except Exception as exc:  # noqa: BLE001
            return name, str(exc)

    errors = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        for name, error in pool.map(fetch, names):
            if error:
                errors.append((name, error))
    if errors:
        raise SystemExit(f"failed {len(errors)} files: {errors[:5]}")
    print(f"wrote {len(names)} files to {out}")


if __name__ == "__main__":
    main()
