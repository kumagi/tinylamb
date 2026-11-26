import TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

/-!
The ReadyForQuery boundary commits an implicit block at the end of its own
message. A following message's error can only clear that message's new block,
so previously committed rows remain intact.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_ReadyBoundarySeparatesImplicitTransactions

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

theorem sound : ReadyBoundarySeparatesImplicitTransactions.Goal := by
  intro prior a b
  have hfirst : message ⟨prior, [], .idle⟩ (writes a) = ⟨prior ++ a, [], .idle⟩ := by
    simpa using message_writes_idle prior [] a
  change message (message ⟨prior, [], .idle⟩ (writes a)) (writes b ++ [Cmd.fail]) = ⟨prior ++ a, [], .idle⟩
  rw [hfirst]
  rw [message_writes_then (prior ++ a) [] b [Cmd.fail]]
  simp [message, clear, empty]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_ReadyBoundarySeparatesImplicitTransactions
