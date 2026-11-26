import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
Equal length, equal min/max of the non-NULL payload, and equal NULL presence
do NOT determine the `test` result: the actual membership of the probe value
matters. With `a = [0,1,2]` and `b = [0,2,0]` (same length 3, min 0, max 2,
no NULLs), probing `some 1` yields `some true` for `a` but `some false` for
`b`, so two IN-lists cannot share a cached bitset on those summary stats
alone. All four side conditions hold definitionally and the unequal `test`
results are separated by `decide`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering_SameCardinalityConstantSetCache
open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem refute : ¬ SameCardinalityConstantSetCache.Goal := by
  intro h
  have hne : test [some 0, some 1, some 2] (some 1) ≠
      test [some 0, some 2, some 0] (some 1) := by decide
  exact hne (h [some 0, some 1, some 2] [some 0, some 2, some 0] (some 1)
    rfl rfl rfl rfl)

end TinylambOpt.ResearchProofs.JitKernels.InListLowering_SameCardinalityConstantSetCache
