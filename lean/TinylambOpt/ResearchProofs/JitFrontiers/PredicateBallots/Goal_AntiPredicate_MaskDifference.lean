import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `AntiPredicate.MaskDifference` — `∀ a b, well a → well b → (test.emit a b).yes=a.yes
  &&& zeros b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_AntiPredicate_MaskDifference

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : AntiPredicate.MaskDifference.Goal := by
  intro a b ha hb
  simp [AntiPredicate.test, andBits, negBits]

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_AntiPredicate_MaskDifference
