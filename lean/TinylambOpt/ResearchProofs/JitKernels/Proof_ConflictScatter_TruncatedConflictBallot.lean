import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
import TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_TruncatedConflictBallot

/-! REFUTE: verified ConflictScatter.TruncatedConflictBallot. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_ConflictScatter_TruncatedConflictBallot
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter.TruncatedConflictBallot
theorem refute : ¬ Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_TruncatedConflictBallot.refute
end Nov26.Proof_ConflictScatter_TruncatedConflictBallot
