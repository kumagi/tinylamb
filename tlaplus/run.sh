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
)
# config -> invariant the README promises to trip (fail configs only; configs
# absent here accept any violation, matching the README's "違反" entries).
declare -A EXPECT_INV=(
  [bug_record]=WalBeforeData [bug_record_harm]=CommittedStateCorrect
  [bug_gen]=EvictedPageReadable [bug_forget]=CommittedStateCorrect
)
ORDER=(current current_harm bug_record bug_record_harm fixed bug_stale bug_gen bug_forget bug_nowait fixed_2pages)
TARGETS=("${@:-${ORDER[@]}}")
mkdir -p out || { echo "cannot create output directory: out" >&2; exit 2; }
status=0
for name in "${TARGETS[@]}"; do
  exp="${EXPECT[$name]:-}"
  if [[ -z "$exp" ]]; then echo "unknown config: $name" >&2; status=1; continue; fi
  log="out/$name.log"
  java -XX:+UseParallelGC -cp "$JAR" tlc2.TLC -workers auto -cleanup \
       -metadir "out/states-$name" -config "cfg/$name.cfg" TxSpill.tla >"$log" 2>&1
  rc=$?
  if [[ $rc -eq 0 ]]; then got=pass; else got=fail; fi
  violated=$(grep -oE 'Invariant [A-Za-z0-9_]+ is violated' "$log" | head -1)
  states=$(grep -oE '[0-9,]+ distinct states found' "$log" | tail -1)
  # A pass must also show TLC actually explored states; rc==0 with an empty
  # state space would mean the spec degenerated rather than verified. A
  # fail-expected config must trip the invariant the README promises, not
  # just any invariant (a regression flipping which one trips is a bug).
  inv_ok=1
  if [[ -n "${EXPECT_INV[$name]:-}" ]] &&
     ! grep -q "Invariant ${EXPECT_INV[$name]} is violated" "$log"; then
    inv_ok=0
  fi
  if [[ "$got" == "$exp" && ( $rc -eq 0 || ( -n "$violated" && $inv_ok -eq 1 ) ) ]] &&
     [[ "$got" != pass || "$states" =~ [1-9] ]]; then mark=OK
  else mark=UNEXPECTED; status=1; fi
  printf '%-11s %-13s expected=%-4s got=%-4s rc=%-3s %s %s\n' \
    "$mark" "$name" "$exp" "$got" "$rc" "${violated:-}" "${states:-}"
done
exit $status
