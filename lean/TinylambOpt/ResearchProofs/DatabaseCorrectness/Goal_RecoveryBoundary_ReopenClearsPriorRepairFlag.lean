import TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary

/-!
After a successful repair the log ends exactly at the validated boundary. A
subsequent open therefore observes a clean end independently of the previous
force/writer flags and of the new tail classification, so no writer is needed.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.RecoveryBoundary.Goal_ReopenClearsPriorRepairFlag

open TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary

theorem sound : ReopenClearsPriorRepairFlag.Goal := by
  intro v n t force writer ok t' h
  simp [finish]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.RecoveryBoundary.Goal_ReopenClearsPriorRepairFlag
