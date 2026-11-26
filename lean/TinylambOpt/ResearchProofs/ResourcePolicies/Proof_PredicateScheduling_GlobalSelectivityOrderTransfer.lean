import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.PredicateScheduling.Goal_GlobalSelectivityOrderTransfer

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling


/-! Selection order learned on a whole input need not survive an arbitrary
downstream filter: a costly early row can be removed from the morsel while a
costly late row remains. This file gives a concrete counterexample. -/

namespace Scratch.GlobalSelectivityOrderTransfer

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.GlobalSelectivityOrderTransfer.Goal := by
  unfold TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.GlobalSelectivityOrderTransfer.Goal
  intro h
  let a : Test := { keep := fun _ => false, cost := fun r => if r == 2 then 1 else 0 }
  let b : Test := { keep := fun _ => false, cost := fun r => if r == 1 then 1 else 0 }
  let rs : List Nat := [0, 1, 2]
  let guard : Nat → Bool := fun r => r == 2
  have hle : work [a, b] rs ≤ work [b, a] rs := by decide
  have hsub := h a b rs guard hle
  have hnot : ¬ work [a, b] (rs.filter guard) ≤ work [b, a] (rs.filter guard) := by decide
  exact hnot hsub

end Scratch.GlobalSelectivityOrderTransfer
end TinylambOpt.ResearchProofs.ResourcePolicies.PredicateScheduling.Goal_GlobalSelectivityOrderTransfer
