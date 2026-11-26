import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `Coalesce.TotalDefault` — `∀ a b, b.nul=0 → (test.emit a b).nul=0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Coalesce_TotalDefault

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : Coalesce.TotalDefault.Goal := by
  intro a b h
  simp [Coalesce.test, h]

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Coalesce_TotalDefault
