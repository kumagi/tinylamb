import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-!
Refutation: dropping unselected deltas before the prefix scan changes the
decoded values, because the surviving prefixes lose the contributions of the
discarded deltas. With `b = 0`, `ds = [5,1]` and `keep = fun i => decide (i == 1)`:
the left side decodes only the kept delta, giving `[1]`, while selecting
decoded rows gives `[values 0 [5,1]] !! 1 = [6]`. Filtering before decoding is
only sound together with a compensation of the skipped deltas.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_FilterBeforePrefixDecode
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem refute : ¬ FilterBeforePrefixDecode.Goal := by
  intro h
  exact absurd (h 0 [5,1] (fun i => decide (i == 1))) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_FilterBeforePrefixDecode
