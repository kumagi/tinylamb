#!/usr/bin/env bash
# Run TLC on every config and compare with the expected outcome.
#   ./run.sh                 # all configs
#   ./run.sh fixed bug_gen   # selected configs
set -uo pipefail
cd "$(dirname "$0")" || exit 2
JAR="${TLA2TOOLS_JAR:-tools/tla2tools.jar}"
if [[ ! -f "$JAR" ]]; then
  echo "tla2tools.jar not found at $JAR" >&2
  echo "Download it from https://github.com/tlaplus/tlaplus/releases into tools/," >&2
  echo "or set TLA2TOOLS_JAR=/path/to/tla2tools.jar." >&2
  exit 2
fi
# config -> expected result (pass | fail)
declare -A EXPECT=(
  [current]=pass [current_harm]=pass [bug_record]=fail [bug_record_harm]=fail [fixed]=pass [fixed_2pages]=pass
  [bug_stale]=fail [bug_gen]=fail [bug_forget]=fail [bug_nowait]=fail
  [s3wal_design]=pass [s3wal_design_nofsync]=pass [s3wal_design_w3]=pass
  [s3wal_bug_noncontig]=fail [s3wal_bug_same_epoch]=fail [s3wal_bug_noseal]=fail
  [s3wal_bug_trust412]=fail [s3wal_bug_reuse]=fail [s3wal_bug_ckpt]=fail [s3wal_bug_chase]=fail
  [s3wal_bug_t2race]=fail [s3wal_bug_sealgc]=fail [s3wal_bug_tail]=fail
  [barrier_version]=fail [barrier_version_heap]=pass [barrier_version_gc]=fail [barrier_tree]=pass
  [barrier_tree_late]=fail [barrier_snapshot]=pass [barrier_buffered]=pass
  [objgc_design]=pass [objgc_bug_timebased]=fail [objgc_bug_tail]=fail [objgc_bug_late_seal]=fail
  [objgc_bug_dedupe]=fail [objgc_bug_settle_early]=fail
  [gcreg_design]=pass [gcreg_bug_norecord]=fail [gcreg_bug_stalemark]=fail
  [manifestgc_design]=pass [manifestgc_bug_headfirst]=fail [manifestgc_bug_sibling]=fail
)
# config -> invariant the README promises to trip (fail configs only; configs
# absent here accept any violation, matching the README's "違反" entries).
declare -A EXPECT_INV=(
  [bug_record]=WalBeforeData [bug_record_harm]=CommittedStateCorrect
  [bug_gen]=EvictedPageReadable [bug_forget]=CommittedStateCorrect
  [s3wal_bug_noncontig]=DurableIsOfficial [s3wal_bug_same_epoch]=ActiveWriterAgrees
  [s3wal_bug_noseal]=HistoryMonotone [s3wal_bug_trust412]=DurableIsOfficial
  [s3wal_bug_reuse]=RestoredIsOfficial [s3wal_bug_ckpt]=CkptConsistent
  [s3wal_bug_chase]=ActiveEpochUnsealed [s3wal_bug_t2race]=ActiveEpochUnsealed
  [s3wal_bug_sealgc]=NoLostAck [s3wal_bug_tail]=RestoredIsOfficial
  [barrier_version]=ObservedDurable [barrier_version_gc]=ObservedDurable
  [barrier_tree_late]=ObservedDurable
  [objgc_bug_timebased]=RefsResolve [objgc_bug_tail]=RefsResolve [objgc_bug_late_seal]=RefsResolve
  [objgc_bug_dedupe]=RefsResolve [objgc_bug_settle_early]=RefsResolve
  [gcreg_bug_norecord]=HeldExists [gcreg_bug_stalemark]=HeldExists
  [manifestgc_bug_headfirst]=HeadResolves [manifestgc_bug_sibling]=PublishedChain
)
# config name prefix -> module
module_for() {
  case "$1" in
    s3wal_*) echo S3Wal.tla ;;
    barrier_*) echo ReplyBarrier.tla ;;
    objgc_*) echo ObjectGc.tla ;;
    gcreg_*) echo GcRegistry.tla ;;
    manifestgc_*) echo ManifestGc.tla ;;
    *) echo TxSpill.tla ;;
  esac
}
ORDER=(current current_harm bug_record bug_record_harm fixed bug_stale bug_gen bug_forget bug_nowait fixed_2pages
  s3wal_design s3wal_design_nofsync s3wal_design_w3
  s3wal_bug_noncontig s3wal_bug_same_epoch s3wal_bug_noseal s3wal_bug_trust412 s3wal_bug_reuse
  s3wal_bug_ckpt s3wal_bug_chase s3wal_bug_t2race s3wal_bug_sealgc s3wal_bug_tail
  barrier_version barrier_version_heap barrier_version_gc barrier_tree barrier_tree_late
  barrier_snapshot barrier_buffered
  objgc_design objgc_bug_timebased objgc_bug_tail objgc_bug_late_seal objgc_bug_dedupe objgc_bug_settle_early
  gcreg_design gcreg_bug_norecord gcreg_bug_stalemark
  manifestgc_design manifestgc_bug_headfirst manifestgc_bug_sibling)
TARGETS=("${@:-${ORDER[@]}}")
mkdir -p out || { echo "cannot create output directory: out" >&2; exit 2; }
status=0
for name in "${TARGETS[@]}"; do
  exp="${EXPECT[$name]:-}"
  if [[ -z "$exp" ]]; then echo "unknown config: $name" >&2; status=1; continue; fi
  log="out/$name.log"
  # -noGenerateSpecTE: keep counterexample traces in the log only, so no
  # <Module>_TTrace_*.tla/.bin files are left next to the models.
  java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -workers auto -cleanup -noGenerateSpecTE \
       -metadir "out/states-$name" -config "cfg/$name.cfg" "$(module_for "$name")" >"$log" 2>&1
  rc=$?
  if [[ $rc -eq 0 ]]; then got=pass; else got=fail; fi
  violated=$(grep -oE '(Invariant|Action property) [A-Za-z0-9_]+ is violated' "$log" | head -1)
  states=$(grep -oE '[0-9,]+ distinct states found' "$log" | tail -1)
  # A pass must also show TLC actually explored states; rc==0 with an empty
  # state space would mean the spec degenerated rather than verified. A
  # fail-expected config must trip the invariant the README promises, not
  # just any invariant (a regression flipping which one trips is a bug).
  inv_ok=1
  if [[ -n "${EXPECT_INV[$name]:-}" ]] &&
     ! grep -qE "(Invariant|Action property) ${EXPECT_INV[$name]} is violated" "$log"; then
    inv_ok=0
  fi
  if [[ "$got" == "$exp" && ( $rc -eq 0 || ( -n "$violated" && $inv_ok -eq 1 ) ) ]] &&
     [[ "$got" != pass || "$states" =~ [1-9] ]]; then mark=OK
  else mark=UNEXPECTED; status=1; fi
  printf '%-11s %-21s expected=%-4s got=%-4s rc=%-3s %s %s\n' \
    "$mark" "$name" "$exp" "$got" "$rc" "${violated:-}" "${states:-}"
done
exit $status
