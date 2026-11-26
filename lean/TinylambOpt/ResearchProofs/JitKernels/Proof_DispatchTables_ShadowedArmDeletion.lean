import TinylambOpt.ResearchGoals.JitKernels.DispatchTables
import TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_ShadowedArmDeletion

/-! SOUND: verified DispatchTables.ShadowedArmDeletion. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DispatchTables_ShadowedArmDeletion
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables.ShadowedArmDeletion
theorem sound : Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_ShadowedArmDeletion.sound
end Nov26.Proof_DispatchTables_ShadowedArmDeletion
