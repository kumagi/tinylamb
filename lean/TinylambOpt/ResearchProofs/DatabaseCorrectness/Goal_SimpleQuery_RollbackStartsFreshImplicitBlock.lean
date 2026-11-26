import TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

/-!
ROLLBACK closes the current block inside a multi-statement Simple Query
message. Writes that follow it start a fresh implicit block and are committed
at the message boundary, leaving only the earlier committed rows plus those
new writes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_RollbackStartsFreshImplicitBlock

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

theorem sound : RollbackStartsFreshImplicitBlock.Goal := by
  intro prior a b
  change message ⟨prior, [], .idle⟩ (writes a ++ [.rollback] ++ writes b) = ⟨prior ++ b, [], .idle⟩
  rw [show writes a ++ [.rollback] ++ writes b =
      writes a ++ (Cmd.rollback :: writes b) by simp [List.append_assoc]]
  rw [message_writes_then prior [] a (Cmd.rollback :: writes b)]
  simp [message, clear]
  change message ⟨prior, [], .idle⟩ (writes b) = ⟨prior ++ b, [], .idle⟩
  simpa using message_writes_idle prior [] b

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_RollbackStartsFreshImplicitBlock
