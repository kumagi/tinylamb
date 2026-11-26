import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-! Distinct residues between adjacent leaders do not guarantee distinct
residues globally: two leaders may share a residue with another leader between
them, so a truncated ballot merges their deltas. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_TruncatedConflictBallot

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

theorem refute : ¬ TruncatedConflictBallot.Goal := by
  intro h
  have bad := h [(0, 1), (1, 0), (2, 2)] 2 0 (by native_decide) (by native_decide)
    (by native_decide) (by native_decide)
  simp [delta] at bad

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_TruncatedConflictBallot
