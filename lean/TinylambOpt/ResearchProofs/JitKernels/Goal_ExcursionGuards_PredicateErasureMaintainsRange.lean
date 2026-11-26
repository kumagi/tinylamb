import TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Refutation: erasing terms changes prefix sums, not just samples of the
original path. A safe chunk can contain a negative term followed by a positive
one; keeping only the negative term produces an unsafe filtered scan. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_PredicateErasureMaintainsRange

open TinylambOpt.ResearchGoals.JitKernels.ExcursionGuards

theorem refute : ¬ PredicateErasureMaintainsRange.Goal := by
  intro h
  have hbad := h 0 10 0 [10, -10, 10] (fun v => v < 0) (by native_decide)
  have hc : safe 0 10 0 ([10, -10, 10].filter (fun v => v < 0)) ≠ true := by native_decide
  exact hc hbad

end TinylambOpt.ResearchProofs.JitKernels.Goal_ExcursionGuards_PredicateErasureMaintainsRange
