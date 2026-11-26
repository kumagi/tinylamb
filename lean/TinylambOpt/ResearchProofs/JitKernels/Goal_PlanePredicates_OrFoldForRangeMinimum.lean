import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
A minimum over a range of residuals cannot be recovered by folding bitwise AND:
`Nat.land` is a bitwise operation, not an order minimum. This file refutes the
claim with a two-element example.

Refuted by counterexample: `x = 3`, `xs = [5]`; `3 &&& 5 = 1` but the minimum of
`[3,5]` is `3`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_OrFoldForRangeMinimum

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

theorem refute : ¬ OrFoldForRangeMinimum.Goal := by
  intro h
  have heq := h 3 [5]
  exact (by native_decide : List.foldl Nat.land 3 [5] ≠ [3, 5].min?.getD 0) heq

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_OrFoldForRangeMinimum
