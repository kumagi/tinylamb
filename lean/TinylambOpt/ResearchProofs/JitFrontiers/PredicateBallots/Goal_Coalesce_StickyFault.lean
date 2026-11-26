import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `Coalesce.StickyFault` — `∀ y, test.scalar .fault y=.fault`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Coalesce_StickyFault

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : Coalesce.StickyFault.Goal := by
  intro y
  cases y <;> native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Coalesce_StickyFault
