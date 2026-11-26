import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Refuted by counterexample

- `ConditionalNull.MinimalDemand` — Every requested RHS lane has two possible RHS tags
  that change its observable result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ConditionalNull_MinimalDemand

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem refute : ¬ ConditionalNull.MinimalDemand.Goal := by
  intro h
  have hh := h (⟨1,0,1⟩ : Bits) 0
  have hleft : (well (⟨1,0,1⟩ : Bits) → 0 < 64 → (ConditionalNull.test.demand (⟨1,0,1⟩ : Bits)).getLsbD 0 = true) := by
    intro hw
    simp [well] at hw
  have hright : ¬ ∃ u v : T, ConditionalNull.test.scalar (lane (⟨1,0,1⟩ : Bits) 0) u ≠ ConditionalNull.test.scalar (lane (⟨1,0,1⟩ : Bits) 0) v := by
    rintro ⟨u,v,hne⟩
    apply hne
    cases u <;> cases v <;> native_decide
  exact hright (hh.mp hleft)

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ConditionalNull_MinimalDemand
