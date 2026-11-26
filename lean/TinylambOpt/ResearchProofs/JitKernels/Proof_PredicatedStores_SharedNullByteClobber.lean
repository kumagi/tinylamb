import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_SharedNullByteClobber
open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-- REFUTED: an inactive validity lane does not suppress the zero it forces.
With `m = fun _ => 5`, `a = 0`, `xs = [none]`, `i = 0` the zero-filled store
writes `0` at byte `0` while `m 0 = 5`, so the live byte is clobbered. -/
theorem refute : ¬ SharedNullByteClobber.Goal := by
  intro h
  exact absurd (h (fun _ => 5) 0 [none] 0 (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_SharedNullByteClobber
