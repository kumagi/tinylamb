import TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

namespace TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.RestartSeekMatchesPrefix

open TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory

private theorem sum_take_drop (xs : List Nat) (a b : Nat) :
    (xs.take a).sum + ((xs.drop a).take b).sum = (xs.take (a + b)).sum := by
  induction xs generalizing a b with
  | nil => simp
  | cons x xs ih =>
      cases a with
      | zero => simp
      | succ a =>
          simp only [List.take, List.drop, List.sum_cons, Nat.succ_add]
          have := ih a b
          omega

theorem goal : RestartSeekMatchesPrefix.Goal := by
  intro ns s i _ hi
  simp only [RestartSeekMatchesPrefix.Goal, seek, offset, anchor]
  have h1 : i / s * s + (i - i / s * s) = i := by
    have := Nat.div_mul_le_self i s
    omega
  have hsum := sum_take_drop ns (i / s * s) (i - i / s * s)
  rw [h1] at hsum
  exact hsum

end TinylambOpt.ResearchProofs.ResourcePolicies.RestartDirectory.RestartSeekMatchesPrefix
