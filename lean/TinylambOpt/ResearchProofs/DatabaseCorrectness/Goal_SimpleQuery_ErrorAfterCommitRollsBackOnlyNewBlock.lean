import TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

/-!
A late error in a Simple Query message aborts only the implicit block opened
after an explicit COMMIT. The earlier commit is already part of the client's
committed rows, so the message boundary leaves exactly those rows visible.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_ErrorAfterCommitRollsBackOnlyNewBlock

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

theorem sound : ErrorAfterCommitRollsBackOnlyNewBlock.Goal := by
  intro prior a b tail
  change message ⟨prior, [], .idle⟩ (writes a ++ [.commit] ++ writes b ++ Cmd.fail :: tail) = ⟨prior ++ a, [], .idle⟩
  rw [show writes a ++ [.commit] ++ writes b ++ .fail :: tail =
      writes a ++ (Cmd.commit :: (writes b ++ Cmd.fail :: tail)) by simp [List.append_assoc]]
  rw [message_writes_then prior [] a (Cmd.commit :: (writes b ++ Cmd.fail :: tail))]
  simp [message, flush, clear, empty]
  rw [message_writes_then (prior ++ a) [] b (Cmd.fail :: tail)]
  simp [message, clear, empty]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_ErrorAfterCommitRollsBackOnlyNewBlock
