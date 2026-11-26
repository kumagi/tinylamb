import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PredicateScheduling_GlobalSelectivityOrderTransfer
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.GlobalSelectivityOrderTransfer

theorem refute : ¬ Goal := by
  intro h
  let a : Test := ⟨fun r => r == 0 || r == 1, fun _ => 1⟩
  let b : Test := ⟨fun r => r == 0 || r == 2, fun _ => 1⟩
  let rs : List Nat := [0, 1, 2]
  let guard : Nat → Bool := fun r => r != 2
  have hglob : work [a, b] rs ≤ work [b, a] rs := by native_decide
  have hbad := h a b rs guard hglob
  simp [work, a, b, rs, guard] at hbad
end Nov26.Proof_PredicateScheduling_GlobalSelectivityOrderTransfer
