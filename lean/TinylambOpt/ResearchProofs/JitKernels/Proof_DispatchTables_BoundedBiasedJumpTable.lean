import TinylambOpt.ResearchGoals.JitKernels.DispatchTables
import TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_BoundedBiasedJumpTable

/-! SOUND: verified DispatchTables.BoundedBiasedJumpTable. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DispatchTables_BoundedBiasedJumpTable
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables.BoundedBiasedJumpTable
theorem sound : Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_BoundedBiasedJumpTable.sound
end Nov26.Proof_DispatchTables_BoundedBiasedJumpTable
