import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

-- Refutation: saturating natural subtraction breaks the biased threshold equivalence.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_BiasedResidualThreshold

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.PlanePredicates.BiasedResidualThreshold.Goal := by
  intro h
  have c := h 5 0 3
  exact (by native_decide : ¬ ((0 ≤ 3 - 5) ↔ (5 + 0 ≤ 3))) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_BiasedResidualThreshold
