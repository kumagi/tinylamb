import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
import TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_LeaderReducedScatter

/-! SOUND: verified ConflictScatter.LeaderReducedScatter. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_ConflictScatter_LeaderReducedScatter
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter.LeaderReducedScatter
theorem sound : Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_LeaderReducedScatter.sound
end Nov26.Proof_ConflictScatter_LeaderReducedScatter
