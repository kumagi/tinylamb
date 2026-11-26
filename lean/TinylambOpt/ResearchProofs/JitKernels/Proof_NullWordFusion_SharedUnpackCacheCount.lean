import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-!
**Refuted: unpacked validity cache keyed only by null count.** Words `1` and
`2` carry one NULL apiece, so their null counts agree, but selection `[0]`
sees different validity per lane. SQL execution cannot reuse an unpacked
validity vector from equal null counts without also certifying equal masks.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_SharedUnpackCacheCount
open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem refute : ¬ SharedUnpackCacheCount.Goal := by
  intro h
  have hcnt : (ords (1 : W)).length = (ords (2 : W)).length := by decide
  have hh := h (1 : W) (2 : W) [0] hcnt
  exact (by decide : ¬ gather (1 : W) [0] = gather (2 : W) [0]) hh

end TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_SharedUnpackCacheCount
