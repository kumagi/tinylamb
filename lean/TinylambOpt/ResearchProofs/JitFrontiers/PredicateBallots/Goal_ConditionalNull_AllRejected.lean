import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `ConditionalNull.AllRejected` — `∀ b, (test.emit ⟨0,0,0⟩ b).nul= -1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ConditionalNull_AllRejected

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : ConditionalNull.AllRejected.Goal := by
  intro b
  simp [ConditionalNull.test, zeros]

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ConditionalNull_AllRejected
