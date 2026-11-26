import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q31_PackedEngineIpKeyBijectionGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

private theorem inj_pair (w x y a b : Nat) (hy : y < w) (hb : b < w)
    (h : x * w + y = a * w + b) : x = a ∧ y = b := by
  rcases Nat.lt_trichotomy x a with h1 | h1 | h1
  · exfalso
    have t := Nat.mul_le_mul_right w (show x + 1 ≤ a by omega)
    rw [Nat.add_one_mul] at t
    omega
  · subst h1
    exact ⟨rfl, by omega⟩
  · exfalso
    have t := Nat.mul_le_mul_right w (show a + 1 ≤ x by omega)
    rw [Nat.add_one_mul] at t
    omega

theorem sound : Q31_PackedEngineIpKeyBijectionGoal := by
  intro e1 ip1 e2 ip2 hip1 hip2 h
  exact inj_pair (2 ^ 32) e1 ip1 e2 ip2 hip1 hip2 h

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q31_PackedEngineIpKeyBijectionGoal
