import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# PairwiseRatioBoundsFromExtrema

Under the positive-value assumptions, bounding the minimum and maximum is sufficient to
bound all modeled pairwise ratios.

A filter over channel totals can test the extremes and avoid evaluating each pair
separately.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.PairwiseRatioBoundsFromExtrema
theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.PairwiseRatioBoundsFromExtrema.Goal := by
  intro a b c
  dsimp
  let m := min a (min b c)
  let M := max a (max b c)
  have hm_mem : m ∈ [a,b,c] := by
    dsimp [m]
    by_cases h1 : a ≤ min b c
    · rw [Nat.min_eq_left h1]
      simp
    · have h1lt : min b c < a := Nat.lt_of_not_ge h1
      have h1' : min b c ≤ a := Nat.le_of_lt h1lt
      rw [Nat.min_eq_right h1']
      by_cases h2 : b ≤ c
      · rw [Nat.min_eq_left h2]
        simp
      · have h2lt : c < b := Nat.lt_of_not_ge h2
        have h2' : c ≤ b := Nat.le_of_lt h2lt
        rw [Nat.min_eq_right h2']
        simp
  have hM_mem : M ∈ [a,b,c] := by
    dsimp [M]
    by_cases h1 : max b c ≤ a
    · rw [Nat.max_eq_left h1]
      simp
    · have h1lt : a < max b c := Nat.lt_of_not_ge h1
      have h1' : a ≤ max b c := Nat.le_of_lt h1lt
      rw [Nat.max_eq_right h1']
      by_cases h2 : c ≤ b
      · rw [Nat.max_eq_left h2]
        simp
      · have h2lt : b < c := Nat.lt_of_not_ge h2
        have h2' : b ≤ c := Nat.le_of_lt h2lt
        rw [Nat.max_eq_right h2']
        simp
  have hm_le : ∀ x ∈ [a,b,c], m ≤ x := by
    intro x hx
    dsimp [m]
    simp at hx
    rcases hx with rfl | rfl | rfl
    · exact Nat.min_le_left _ _
    · exact Nat.le_trans (Nat.min_le_right _ _) (Nat.min_le_left _ _)
    · exact Nat.le_trans (Nat.min_le_right _ _) (Nat.min_le_right _ _)
  have h_le_M : ∀ x ∈ [a,b,c], x ≤ M := by
    intro x hx
    dsimp [M]
    simp at hx
    rcases hx with rfl | rfl | rfl
    · exact Nat.le_max_left _ _
    · exact Nat.le_trans (Nat.le_max_left _ _) (Nat.le_max_right _ _)
    · exact Nat.le_trans (Nat.le_max_right _ _) (Nat.le_max_right _ _)
  constructor
  · intro h
    have h1 := h M hM_mem m hm_mem
    have h2 := h m hm_mem M hM_mem
    omega
  · intro h x hx y hy
    have hmx := hm_le x hx
    have hMy := h_le_M y hy
    have hmY := hm_le y hy
    have hMx := h_le_M x hx
    omega
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.PairwiseRatioBoundsFromExtrema
