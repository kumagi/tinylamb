# `sql_ooc_fuzzer` open findings

Minimized traces of findings the engine **still fails**.  Nothing in ctest
replays this directory; it is the list of known bugs for campaigns:

```bash
# which filed findings still fail, which are fixed
scripts/ooc_fuzz.py replay --bin build/sql_ooc_fuzz query/testdata/ooc_open_findings

# hunt for new ones: buckets filed here are labelled `known` and do not fail the run
scripts/ooc_fuzz.py run --bin build/sql_ooc_fuzz --out /tmp/ooc --minutes 30 \
    --known query/testdata/ooc_open_findings --minimize
```

- File a finding as `YYYYMMDD_<what>.test`, minimized, keeping the
  `-- failure: <signature> :: <report>` header `scripts/ooc_fuzz.py` wrote —
  `--known` matches on that signature.
- `replay` says `FIXED`: move the trace to `../ooc_regressions/` (ctest
  replays that directory, so never move a trace that still fails).
- `replay` says `CHANGED`: the trace now fails with another signature;
  minimize it again and refile.
- The remote campaign (`scripts/oci_fuzz.sh deploy`) reads this directory
  too, so a filed finding stops being reported as `NEW` after the next deploy.

Workflow and signature reference: [`docs/ooc_fuzzing.md`](../../../docs/ooc_fuzzing.md).
