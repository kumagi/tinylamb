import TinylambOpt.ResearchGoals.JitKernels.DispatchTables
import TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_SplitTableAtHotBoundary

/-! SOUND: verified DispatchTables.SplitTableAtHotBoundary. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_DispatchTables_SplitTableAtHotBoundary
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables
open TinylambOpt.ResearchGoals.JitKernels.DispatchTables.SplitTableAtHotBoundary
theorem sound : Goal := by
  exact TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_SplitTableAtHotBoundary.sound
end Nov26.Proof_DispatchTables_SplitTableAtHotBoundary
