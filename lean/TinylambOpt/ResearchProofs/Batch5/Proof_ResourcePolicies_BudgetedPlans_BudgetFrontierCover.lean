import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.BudgetedPlans.Goal_BudgetFrontierCover

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans


/-!
  Budgeted plan frontier cover: retaining a profile that dominates every omitted
  profile preserves the minimum work among plans that fit any memory cap.  The
  proof abstracts the feasible-cost lists to natural numbers and compares their
  `min?` values; SQL execution may drop dominated build-side candidates only
  when the retained candidate fits whenever the dropped one fits.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

theorem min_eq_of_subset_of_dominated {l₁ l₂ : List Nat}
    (hsub : ∀ x, x ∈ l₁ → x ∈ l₂)
    (hdom : ∀ x, x ∈ l₂ → ∃ y, y ∈ l₁ ∧ y ≤ x) : l₁.min? = l₂.min? := by
  by_cases h2nil : l₂ = []
  · have h1nil : l₁ = [] := by
      cases l₁ with
      | nil => rfl
      | cons x xs =>
        exfalso
        have : x ∈ l₂ := hsub x (by simp)
        simp [h2nil] at this
    simp [h1nil, h2nil]
  · let m := l₂.min h2nil
    have hmMem : m ∈ l₂ := List.min_mem h2nil
    rcases hdom m hmMem with ⟨y, hy1, hym⟩
    have hmy : m = y := by
      exact Nat.le_antisymm (List.min_le_of_mem (hsub y hy1)) hym
    have hyLe : ∀ x, x ∈ l₁ → y ≤ x := by
      intro x hx
      exact Nat.le_trans hym (List.min_le_of_mem (hsub x hx))
    have hmin1 : l₁.min? = some y := (List.min?_eq_some_iff).2 ⟨hy1, hyLe⟩
    have hmin2 : l₂.min? = some y := by
      have : l₂.min? = some m := List.min?_eq_some_min h2nil
      simpa [hmy] using this
    rw [hmin1, hmin2]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans.BudgetFrontierCover.Goal := by
  intro all kept hsub hdom cap
  unfold cheapest
  apply min_eq_of_subset_of_dominated
  · intro x hx
    simp at hx ⊢
    rcases hx with ⟨p, ⟨hpk, hpeak⟩, rfl⟩
    exact ⟨p, ⟨hsub p hpk, hpeak⟩, rfl⟩
  · intro x hx
    simp at hx
    rcases hx with ⟨p, ⟨hpa, hpeak⟩, rfl⟩
    rcases hdom p hpa with ⟨q, hqk, hqdom⟩
    have hqpeak : q.peak ≤ cap := Nat.le_trans hqdom.2.2.1 hpeak
    refine ⟨q.work, ?_, hqdom.1⟩
    simp
    exact ⟨q, ⟨hqk, hqpeak⟩, rfl⟩
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.BudgetedPlans.Goal_BudgetFrontierCover
