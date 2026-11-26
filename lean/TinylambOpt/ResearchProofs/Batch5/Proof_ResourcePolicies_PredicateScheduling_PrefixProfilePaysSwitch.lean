import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_PrefixProfilePaysSwitch

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling


/-! A prefix can favor one filter order for reasons that disappear on the tail:
the later rows are cheap for the rejected filter and expensive for the other.
This file gives a concrete counterexample. -/

namespace Scratch.PrefixProfilePaysSwitch

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.PrefixProfilePaysSwitch.Goal := by
  unfold TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.PrefixProfilePaysSwitch.Goal
  intro h
  let a : Test := { keep := fun r => r == 0, cost := fun r => if r == 0 then 10 else 0 }
  let b : Test := { keep := fun r => r == 1, cost := fun r => if r == 1 then 20 else 0 }
  let rs : List Nat := [0, 1]
  have hpref : work [b, a] (rs.take 1) + 0 < work [a, b] (rs.take 1) := by decide
  have hfull := h a b rs 1 0 hpref
  have hnot : ¬ work [b, a] rs + 0 < work [a, b] rs := by decide
  exact hnot hfull

end Scratch.PrefixProfilePaysSwitch
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_PrefixProfilePaysSwitch
