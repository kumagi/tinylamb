import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PlanePredicates_DistinctMaskFromOr
open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-- REFUTED: an OR synopsis keeps only the union of bit positions. `a = [1,2]` and
`b = [3,0]` both OR-fold to `3`, but the distinct-value sets `{1,2}` and `{3,0}`
differ, so `eraseDups` is not determined by the OR mask. -/
theorem refute : ¬ DistinctMaskFromOr.Goal := by
  intro h
  have hc := h [1, 2] [3, 0] rfl (by decide)
  rw [List.perm_iff_count] at hc
  exact absurd (hc 1) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.PlanePredicates_DistinctMaskFromOr
