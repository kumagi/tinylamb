import TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `Disjunction.TrueSuppresses` — `∀ b, well b → (test.emit ⟨-1,0,0⟩ b).err=0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Disjunction_TrueSuppresses

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots

theorem sound : Disjunction.TrueSuppresses.Goal := by
  intro b hb
  simp [Disjunction.test, andBits, negBits, zeros]

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Disjunction_TrueSuppresses
