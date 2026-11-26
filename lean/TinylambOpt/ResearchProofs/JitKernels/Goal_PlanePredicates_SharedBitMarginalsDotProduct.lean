import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
Per-column bit populations do not determine a dot product: permuting one input
relative to the other changes which values are paired even though both inputs
have the same multiset. This file refutes the claim that a shared marginal
synopsis is enough.

Refuted by counterexample: `a = [0,0,1]`, `b = [0,0,1]`, `c = [0,1,0]`; `b` and
`c` are permutations, but `a·b = 1` while `a·c = 0`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_SharedBitMarginalsDotProduct

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

theorem refute : ¬ SharedBitMarginalsDotProduct.Goal := by
  intro h
  have heq := h [0, 0, 1] [0, 0, 1] [0, 1, 0] (by native_decide) (by native_decide)
  exact (by native_decide :
    ([0, 0, 1].zipWith (fun x y => x * y) [0, 0, 1]).sum ≠
      ([0, 0, 1].zipWith (fun x y => x * y) [0, 1, 0]).sum) heq

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_SharedBitMarginalsDotProduct
