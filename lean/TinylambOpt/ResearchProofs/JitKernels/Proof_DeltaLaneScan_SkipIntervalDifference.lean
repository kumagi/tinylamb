import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-!
The difference of two decoded rows needs only the deltas strictly between them:
for `i ≤ j < ds.length`, `get b ds j - get b ds i` equals the sum of
`ds.drop (i+1) |> take (j-i)` — the bases and the shared prefix cancel.
Skip-scans over delta columns can therefore subtract zone-restricted runs
without materializing either row's full prefix sum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_SkipIntervalDifference
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem sound : SkipIntervalDifference.Goal := by
  intro b ds i j hij hj
  unfold TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.get
  rw [show j + 1 = i + 1 + (j - i) from by omega, List.take_add, List.sum_append]
  omega

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_SkipIntervalDifference
