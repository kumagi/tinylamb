import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-!
Adjacent distinct residues of consecutive leaders modulo `m` do not separate
all buckets: for `us = (1,5) :: (2,4) :: (3,7) :: []` and `m = 2` the leaders
`[1,2,3]` have alternating residues mod 2, yet keys `1` and `3` collide after
truncation, so the truncated ballot folds slot 3's `7` into slot `1 % 2`. The
claim `TruncatedConflictBallot.Goal` is therefore false.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_TruncatedConflictBallot
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

theorem refute : ¬ TruncatedConflictBallot.Goal := by
  intro h
  have := h [(1, 5), (2, 4), (3, 7)] 2 1
  simp only [TruncatedConflictBallot.Goal] at this
  have hne : delta ((([(1, 5), (2, 4), (3, 7)] : Updates).map fun u => (u.1 % 2, u.2))) (1 % 2)
      ≠ delta [(1, 5), (2, 4), (3, 7)] 1 := by decide
  exact hne (this (by decide) (by decide) (by decide) (by decide))

end TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_TruncatedConflictBallot
