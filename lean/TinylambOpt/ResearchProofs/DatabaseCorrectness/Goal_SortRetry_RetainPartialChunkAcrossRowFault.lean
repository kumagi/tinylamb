import TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

/-!
Keeping the already emitted prefix and requesting only the remaining capacity
reconstructs the original chunk exactly. SQL may therefore resume a chunk
after a row fault without re-reading or duplicating the committed prefix.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_RetainPartialChunkAcrossRowFault

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

theorem sound : RetainPartialChunkAcrossRowFault.Goal := by
  intro rs
  induction rs with
  | nil => intro k n hle; simp
  | cons x xs ih =>
      intro k n hle
      cases k with
      | zero => simp
      | succ k' =>
          cases n with
          | zero => omega
          | succ n' =>
              simpa using ih k' n' (by omega)

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_RetainPartialChunkAcrossRowFault
