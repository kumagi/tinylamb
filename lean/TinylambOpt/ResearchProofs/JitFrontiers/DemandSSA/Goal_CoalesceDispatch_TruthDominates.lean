import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

/-!
SQL expression lowering must preserve which values and errors are demanded on each
active control-flow path.

Proved

- `CoalesceDispatch.TruthDominates` — A successful predicate can authorize a later
  branch without a second scalar guard.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_CoalesceDispatch_TruthDominates

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem sound : CoalesceDispatch.TruthDominates.Goal := by
  intro e hg
  cases h0 : slot e 0 <;> cases h1 : slot e 1 <;> cases h2 : slot e 2 <;>
    simp [slot] at h0 h1 h2 <;>
    simp [slot, run, after, dispatch, bne, instBEqV.beq, CoalesceDispatch.rule, usable, present, h0, h1, h2] at hg ⊢

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_CoalesceDispatch_TruthDominates
