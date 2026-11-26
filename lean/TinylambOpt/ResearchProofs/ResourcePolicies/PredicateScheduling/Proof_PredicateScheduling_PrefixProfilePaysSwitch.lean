import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PredicateScheduling_PrefixProfilePaysSwitch
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.PrefixProfilePaysSwitch

theorem refute : ¬ Goal := by
  intro h
  let a : Test := ⟨fun r => r == 0, fun _ => 1⟩
  let b : Test := ⟨fun r => r == 1, fun _ => 1⟩
  let rs : List Nat := [0, 1]
  have hpfx : work [b, a] (rs.take 1) + 0 < work [a, b] (rs.take 1) := by native_decide
  have hbad := h a b rs 1 0 hpfx
  simp [work, a, b, rs] at hbad
end Nov26.Proof_PredicateScheduling_PrefixProfilePaysSwitch
