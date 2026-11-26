import TinylambOpt.ResearchGoals.RestartRaces.Concurrency
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Concurrency_RangePromotion

open TinylambOpt.ResearchGoals.RestartRaces.Concurrency.RangePromotion

private theorem count_le_sum {α : Type} (l : List α) (f : α → Nat) :
    l.countP (fun x => f x != 0) ≤ (l.map f).sum := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.countP_cons, List.map_cons, List.sum_cons]
      by_cases h : f a = 0
      · simp [h]
        exact ih
      · simp [h]
        omega

private theorem sum_le_d_mul_count {α : Type} (l : List α) (f : α → Nat) (d : Nat)
    (h : ∀ x ∈ l, f x ≤ d) :
    (l.map f).sum ≤ d * l.countP (fun x => f x != 0) := by
  induction l with
  | nil => simp
  | cons a l ih =>
      simp only [List.map_cons, List.sum_cons, List.countP_cons]
      have ha : f a ≤ d := h a (by simp)
      have hi : (l.map f).sum ≤ d * l.countP (fun x => f x != 0) :=
        ih (fun x hx => h x (by simp [hx]))
      by_cases hz : f a = 0
      · simp [hz]
        exact hi
      · simp [hz, Nat.mul_succ]
        omega

theorem sound : Goal := by
  intro ws covers chosen d hchosen hbound hmin c hc
  have h1 : exact ws chosen ≤ additive ws chosen := by
    dsimp [exact, additive]
    apply count_le_sum
  have h2 : additive ws chosen ≤ additive ws c := hmin c hc
  have h3 : additive ws c ≤ d * exact ws c := by
    dsimp [additive, exact]
    apply sum_le_d_mul_count
    intro w hw
    exact hbound c hc w hw
  exact Nat.le_trans h1 (Nat.le_trans h2 h3)

end TinylambOpt.ResearchProofs.RestartRaces.Concurrency_RangePromotion
