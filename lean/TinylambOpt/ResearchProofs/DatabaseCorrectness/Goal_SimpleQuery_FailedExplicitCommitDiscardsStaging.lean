import TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

/-!
An error inside an explicit transaction suppresses the rest of that message
and marks it failed. A COMMIT in the next message then clears the staging area
instead of committing it, so no staged row from the failed block survives.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_FailedExplicitCommitDiscardsStaging

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

theorem sound : FailedExplicitCommitDiscardsStaging.Goal := by
  intro prior a tail
  dsimp
  rw [message_writes_then_explicit prior [] a (Cmd.fail :: tail)]
  simp only [message]
  constructor
  · rfl
  · simp [message, clear, flush, empty]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_FailedExplicitCommitDiscardsStaging
