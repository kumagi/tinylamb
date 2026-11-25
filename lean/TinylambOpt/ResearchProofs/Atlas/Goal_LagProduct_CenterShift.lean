import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

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
