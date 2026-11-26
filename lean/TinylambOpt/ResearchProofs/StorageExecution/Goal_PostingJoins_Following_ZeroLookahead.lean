import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

/-!
Posting-list execution can use key and interval structure to reduce the rows it examines
while preserving the modeled join or count.

Proved

- `Following.ZeroLookahead` — `∀ u x, search.spans u 0 x = []`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Following_ZeroLookahead

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Following

/-- Truth check on small bounded inputs: with zero slack the span is empty. -/
private def boundedCheck : Bool :=
  (List.range 12).all fun u =>
    (List.range 12).all fun x =>
      decide (search.spans u 0 x == [])

#eval boundedCheck

theorem sound_ZeroLookahead : Following.ZeroLookahead.Goal := by
  intro u x
  change (if x + 1 < min u (x + 1) then [(x + 1, min u (x + 1))] else []) = []
  split
  · have hle : min u (x + 1) ≤ x + 1 := Nat.min_le_right _ _
    omega
  · rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Following_ZeroLookahead
