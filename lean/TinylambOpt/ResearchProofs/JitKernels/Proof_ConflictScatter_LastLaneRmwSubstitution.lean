import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
import TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_LastLaneRmwSubstitution

/-! REFUTE: verified ConflictScatter.LastLaneRmwSubstitution. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_ConflictScatter_LastLaneRmwSubstitution
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter.LastLaneRmwSubstitution
theorem refute : ¬ Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_LastLaneRmwSubstitution.refute
end Nov26.Proof_ConflictScatter_LastLaneRmwSubstitution
