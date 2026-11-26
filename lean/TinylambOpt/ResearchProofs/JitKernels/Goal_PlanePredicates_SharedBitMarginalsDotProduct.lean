import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_SharedBitMarginalsDotProduct

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-! Permuting one operand changes the dot product even though its per-column
bit populations are unchanged. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.PlanePredicates.SharedBitMarginalsDotProduct.Goal := by
  rw [SharedBitMarginalsDotProduct.Goal]
  intro h
  have hh := h [1,2] [1,2] [2,1] (by decide) (by decide)
  have hbad : 5 ≠ 4 := by decide
  exact hbad hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_SharedBitMarginalsDotProduct
