import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

/-!
SQL expression lowering must preserve which values and errors are demanded on each
active control-flow path.

Proved

- `AndCommute.CostCriterion` — `∀ e, present e 0 → present e 1 → ((run e
  rule.dst).2.length < (run e rule.src).2.length ↔ slot e 1=.no ∧ slot e 0=.yes)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AndCommute_CostCriterion

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem sound : AndCommute.CostCriterion.Goal := by
  intro e hp0 hp1
  cases h0 : slot e 0 <;> cases h1 : slot e 1 <;> cases h2 : slot e 2 <;>
    simp [slot] at h0 h1 h2 <;>
    simp [slot, run, after, dispatch, bne, instBEqV.beq, AndCommute.rule, usable, present, h0, h1, h2] at hp0 hp1 ⊢

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AndCommute_CostCriterion
