import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-!
Refutation: an independent per-lane accumulator misses the deltas encoded on
the other lanes earlier in the row order. With `b = 0`, `ds = [1,1]`, `m = 2`,
`i = 1` (nonnegative deltas, `0 < m`, `i < ds.length`): the lane-1 strided scan
sees only position 1 and yields `strideScan 2 0 [1,1] 1 = 1`, whereas
`get 0 [1,1] 1 = 2`. Lane isolation requires cross-lane carry propagation.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_CrossLanePrefixIsolation
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem refute : ¬ CrossLanePrefixIsolation.Goal := by
  intro h
  exact absurd (h 0 [1,1] 1 2 (by decide) (by decide) (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_CrossLanePrefixIsolation
