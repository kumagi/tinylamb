import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
Turning an inactive lane into bucket zero changes the same-bucket rank of later
bucket-zero lanes. Rewriting NULLs to zero before histogramming is not a
destination-preserving simplification for the SQL scatter kernel.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_IgnoredNullAsBucketZero

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

theorem refute : ¬ IgnoredNullAsBucketZero.Goal := by
  intro h
  have got := h [none, some 0] 1 0
  simp [dst, active, base, rank] at got

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_IgnoredNullAsBucketZero
