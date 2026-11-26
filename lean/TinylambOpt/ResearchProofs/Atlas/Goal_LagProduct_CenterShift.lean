import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `LagProduct.CenterShift` — `∀ x b, x ≠ [] → measure (shifted b x) = measure x +
  b*(2*x.sum-x.head?.getD 0-x.getLast?.getD 0) + b^2*((x.length-1:Nat):Int)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_LagProduct_CenterShift

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.LagProduct

theorem sound_LagProduct_CenterShift : LagProduct.CenterShift.Goal := by
  intro x b hx
  induction x with
  | nil => contradiction
  | cons a xs ih =>
      cases xs with
      | nil =>
          simp [LagProduct.measure, shifted, adj]
          grind
      | cons c ys =>
          have hne : c :: ys ≠ ([] : Series) := by simp
          have hih := ih hne
          simp [LagProduct.measure, shifted, adj, List.getLast?_cons_of_ne_nil hne] at hih ⊢
          grind

end TinylambOpt.ResearchProofs.Atlas.Goal_LagProduct_CenterShift
