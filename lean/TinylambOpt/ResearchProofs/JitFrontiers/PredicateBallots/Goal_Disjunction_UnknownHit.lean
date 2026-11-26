import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `Disjunction.UnknownHit` — `test.scalar .null .yes=.yes`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Disjunction_UnknownHit

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : Disjunction.UnknownHit.Goal := by
  unfold Disjunction.UnknownHit.Goal
  native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Disjunction_UnknownHit
