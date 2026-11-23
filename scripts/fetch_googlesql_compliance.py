#!/usr/bin/env python3
"""Refresh query/testdata/googlesql_compliance from google/googlesql.

Omits differential-privacy / anonymization files. Origin is Apache-2.0.
"""

from __future__ import annotations

import json
import os
import re
import threading
import tempfile
import time
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
    # A hard process exit can leave an atomic-download scratch file behind.
    # Requests below time out after 60 seconds, so a day-old partial is stale
    # even if another fetch process is still running.
    stale_before = time.time() - 24 * 60 * 60
    for partial in out.glob("*.test.part.*"):
        try:
            if partial.is_file() and partial.stat().st_mtime < stale_before:
                partial.unlink()
        except OSError:
            pass
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

    stage_root = Path(tempfile.mkdtemp(prefix=".googlesql-fetch-", dir=out))

    def fetch(name: str) -> tuple[str, str | None]:
        dest = stage_root / name
        try:
            request = urllib.request.Request(raw + name, headers=HEADERS)
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = response.read()
            tmp = dest.with_name(
                dest.name + f".part.{os.getpid()}.{threading.get_ident()}")
            try:
                tmp.write_bytes(payload)
                os.replace(tmp, dest)
            finally:
                tmp.unlink(missing_ok=True)
            return name, None
        except Exception as exc:  # noqa: BLE001
            return name, str(exc)

    errors = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        for name, error in pool.map(fetch, names):
            if error:
                errors.append((name, error))
    if errors:
        # Staged downloads are discarded, so failed requests leave the checked-in
        # testdata untouched.
        for staged in stage_root.iterdir():
            staged.unlink(missing_ok=True)
        stage_root.rmdir()
        raise SystemExit(f"failed {len(errors)} files: {errors[:5]}")
    for name in names:
        os.replace(stage_root / name, out / name)
    stage_root.rmdir()
    # Prune only files this script could have fetched (upstream *.test).
    # Other files in the directory are local fixtures/documentation; deleting
    # them would silently break tests that reference them.
    for existing in out.iterdir():
        if not existing.is_file() or not existing.name.endswith(".test"):
            continue
        if (existing.name not in wanted and
                not existing.name.startswith(LOCAL_PREFIXES) and
                existing.name not in LOCAL_FILES):
            existing.unlink()
    print(f"wrote {len(names)} files to {out}")


if __name__ == "__main__":
    main()
