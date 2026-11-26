import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PlanePredicates_SharedBitMarginalsDotProduct
open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-- REFUTED: per-position products depend on how the lanes of `b` are paired with
`a`, not just on the multiset of `b`. With `a = [1,2]`, `b = [0,10]`, `c = [10,0]`
(a permutation of `b`), the dot products are `20` and `10`. -/
theorem refute : ¬ SharedBitMarginalsDotProduct.Goal := by
  intro h
  exact absurd (h [1, 2] [0, 10] [10, 0] rfl (List.Perm.swap 0 10 []).symm) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.PlanePredicates_SharedBitMarginalsDotProduct
