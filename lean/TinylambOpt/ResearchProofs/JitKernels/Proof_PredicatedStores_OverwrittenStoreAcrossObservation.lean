import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_OverwrittenStoreAcrossObservation
open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-- REFUTED: the claim drops the second store. Reading `xs.length` bytes of
`store m a xs` yields the freshly stored bytes, not the old ones: with
`m = fun _ => 0`, `a = 0`, `xs = [1]`, `ys = [1]` the two reads are `[1]` and
`[0]`. -/
theorem refute : ¬ OverwrittenStoreAcrossObservation.Goal := by
  intro h
  exact absurd (h (fun _ => 0) 0 [1] [1] (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_OverwrittenStoreAcrossObservation
