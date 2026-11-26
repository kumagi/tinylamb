import TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary

/-!
A nonzero ambiguous short record followed by a later valid frame is not a
benign tail: truncating there would discard committed work. Normal recovery
therefore reports corruption even when a writer and truncation are available.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.RecoveryBoundary.Goal_LaterValidFramePreventsTailTruncation

open TinylambOpt.ResearchGoals.DatabaseCorrectness.RecoveryBoundary

theorem sound : LaterValidFramePreventsTailTruncation.Goal := by
  intro v n t hvn hnonzero hlater
  simp [finish, benign, hvn, hnonzero, hlater]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.RecoveryBoundary.Goal_LaterValidFramePreventsTailTruncation
