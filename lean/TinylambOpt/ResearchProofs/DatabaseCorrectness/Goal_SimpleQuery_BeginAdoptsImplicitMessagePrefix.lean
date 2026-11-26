import TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
import TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

/-!
BEGIN in the same message as implicit writes adopts those staged rows into the
explicit transaction. A later ROLLBACK message therefore removes both the
implicit prefix and the explicit suffix while leaving earlier committed rows.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_BeginAdoptsImplicitMessagePrefix

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SimpleQuery
open TinylambOpt.ResearchProofs.DatabaseCorrectness.SimpleQueryCommon

theorem sound : BeginAdoptsImplicitMessagePrefix.Goal := by
  intro prior a b
  dsimp
  have h : message ⟨prior, [], .idle⟩ (writes a ++ [.beginTxn] ++ writes b) =
      ⟨prior, a ++ b, .explicitTxn⟩ := by
    rw [show writes a ++ [.beginTxn] ++ writes b =
        writes a ++ (Cmd.beginTxn :: writes b) by simp [List.append_assoc]]
    rw [message_writes_then prior [] a (Cmd.beginTxn :: writes b)]
    simp [message]
    exact message_writes_explicit prior a b
  constructor
  · exact h
  · change message (message ⟨prior, [], .idle⟩ (writes a ++ [.beginTxn] ++ writes b)) [Cmd.rollback] =
      ⟨prior, [], .idle⟩
    rw [h]
    simp [message, clear, flush, empty]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SimpleQuery_BeginAdoptsImplicitMessagePrefix
