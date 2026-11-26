import TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

/-!
A failed refill does not consume a row. Retrying `ready` from the cursor
returned by that failure must therefore emit the same winner and advance the
count identically, so a transaction may retry without losing a row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_RestoredWinnerRowRetry

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

theorem sound : RestoredWinnerRowRetry.Goal := by
  intro s
  rcases s with ⟨rows, emitted⟩
  cases rows with
  | nil => simp [next]
  | cons r rs => simp [next]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_RestoredWinnerRowRetry
