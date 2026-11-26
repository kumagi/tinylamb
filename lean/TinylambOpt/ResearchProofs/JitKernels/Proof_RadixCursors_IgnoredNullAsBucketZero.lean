import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

/-!
Turning an inactive lane into bucket zero changes the same-bucket rank of later
bucket-zero lanes. Rewriting NULLs to zero before histogramming is not a
destination-preserving simplification for the SQL scatter kernel.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unnecessarySimpa false

namespace Nov26.Proof_RadixCursors_IgnoredNullAsBucketZero

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors
open TinylambOpt.ResearchGoals.JitKernels.RadixCursors.IgnoredNullAsBucketZero

theorem refute : ¬ Goal := by
  intro h
  have got := h [none, some 0] 1 0
  simp [dst, active, base, rank] at got

end Nov26.Proof_RadixCursors_IgnoredNullAsBucketZero
