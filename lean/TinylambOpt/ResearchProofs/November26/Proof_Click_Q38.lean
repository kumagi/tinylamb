import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q38
open TinylambOpt.ResearchGoals.November26.Click

theorem sum_le_mul_length (xs : List Nat) (cap : Nat) (h : ∀ n ∈ xs, n ≤ cap) :
    xs.sum ≤ cap * xs.length := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.sum_cons, List.length_cons]
      rw [Nat.mul_succ]
      have hx := h x (by simp)
      have hxs : ∀ n ∈ xs, n ≤ cap := by intro n hn; exact h n (by simp [hn])
      have hi := ih hxs
      omega

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q38.Goal := by
  intro pages candidates cap t h hbig
  by_cases ht : t ∈ candidates
  · exact ht
  · exfalso
    have hle : pages.flatten.count t ≤ cap * pages.length := by
      rw [List.count_flatten]
      simpa using (sum_le_mul_length (pages.map (List.count t)) cap (by
        intro n hn
        simp only [List.mem_map] at hn
        rcases hn with ⟨p, hp, rfl⟩
        by_cases htp : t ∈ p
        · exact h p hp t htp ht
        · have hc : p.count t = 0 := List.count_eq_zero.mpr htp
          simp [hc]))
    omega
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q38
