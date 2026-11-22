#!/usr/bin/env python3
"""Refresh query/testdata/googlesql_compliance from google/googlesql.

Omits differential-privacy / anonymization files. Origin is Apache-2.0.
"""

from __future__ import annotations

import json
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HEADERS = {"User-Agent": "tinylamb-compliance-fetch"}

SKIP = (
    "differential_privacy",
    "anonymization",
    "aggregation_threshold",
    "pipe_aggregate_with_dp",
    # Graph (GQL) queries are out of scope for tinylamb.
    "graph_",
)

LOCAL_PREFIXES = ("optimizer_",)
# Local documentation lives alongside the vendored testdata and is not part of
# the upstream listing; keep it when pruning stale upstream files.
LOCAL_FILES = ("README.md",)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    cmake = (root / "CMakeLists.txt").read_text(encoding="utf-8")
    version = re.search(r'^set\(TINYLAMB_GOOGLESQL_VERSION\s+"([^"]+)"', cmake, re.M)
    if version is None:
        raise SystemExit("TINYLAMB_GOOGLESQL_VERSION is missing from CMakeLists.txt")
    ref = version.group(1)
    repo = (
        "https://api.github.com/repos/google/googlesql/contents/"
        f"googlesql/compliance/testdata?ref={ref}"
    )
    raw = (
        f"https://raw.githubusercontent.com/google/googlesql/{ref}/"
        "googlesql/compliance/testdata/"
    )
    out = root / "query" / "testdata" / "googlesql_compliance"
    out.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(
        repo, headers=HEADERS
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

    def fetch(name: str) -> tuple[str, str | None]:
        dest = out / name
        try:
            request = urllib.request.Request(raw + name, headers=HEADERS)
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
        # Keep the existing testdata intact on any transient failure: deleting
        # first would leave neither old nor new files on a partial download.
        raise SystemExit(f"failed {len(errors)} files: {errors[:5]}")
    # Delete every stale upstream file, not just *.test: upstream may drop a
    # .json (or other) fixture, and leaving the old copy behind silently runs
    # compliance against outdated data.
    for existing in out.iterdir():
        if not existing.is_file():
            continue
        if (existing.name not in wanted and
                not existing.name.startswith(LOCAL_PREFIXES) and
                existing.name not in LOCAL_FILES):
            existing.unlink()
    print(f"wrote {len(names)} files to {out}")


if __name__ == "__main__":
    main()
