import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

namespace TinylambOpt.ResearchProofs.ResourcePolicies.PredicateScheduling.Goal_AcceptanceMaskPreservesErrors

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling


/-! Equal accept/reject masks do not fix the observable error: two filters can
both fault, and swapping them changes which fault code reaches the caller.
This file gives a concrete counterexample. -/

namespace Scratch.AcceptanceMaskPreservesErrors

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.AcceptanceMaskPreservesErrors.Goal := by
  unfold TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.AcceptanceMaskPreservesErrors.Goal
  intro h
  let a : Nat → Outcome := fun _ => .fault 1
  let b : Nat → Outcome := fun _ => .fault 2
  let rs : List Nat := [0]
  have hmask : rs.filter (fun r => evaluate [a, b] r == .pass) =
      rs.filter (fun r => evaluate [b, a] r == .pass) := by decide
  have hout := h a b rs hmask
  have hnot : rs.map (evaluate [a, b]) ≠ rs.map (evaluate [b, a]) := by decide
  exact hnot hout

end Scratch.AcceptanceMaskPreservesErrors
end TinylambOpt.ResearchProofs.ResourcePolicies.PredicateScheduling.Goal_AcceptanceMaskPreservesErrors
