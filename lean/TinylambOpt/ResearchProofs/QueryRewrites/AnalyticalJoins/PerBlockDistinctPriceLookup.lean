import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# PerBlockDistinctPriceLookup

With unique product keys, checking the price predicate once per distinct product in a
sales block preserves the per-row selection. The scan can reuse the selected codes.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.PerBlockDistinctPriceLookup

theorem filter_congr_mem {p q : Nat → Bool} : ∀ (l : List Nat), (∀ x ∈ l, p x = q x) → l.filter p = l.filter q
  | [], _ => by simp
  | a :: t, h => by
      have ha := h a (by simp)
      have ht := filter_congr_mem t (fun x hx => h x (by simp [hx]))
      simp only [List.filter_cons]
      rw [ha]
      by_cases hq : q a = true
      · simp [hq, ht]
      · simp [hq, ht]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.PerBlockDistinctPriceLookup.Goal := by
  intro sold priced
  dsimp only
  apply filter_congr_mem
  intro x hx
  have hxE : x ∈ sold.eraseDups := (List.mem_eraseDups).2 hx
  by_cases hp : priced x = true
  · have hm : x ∈ sold.eraseDups.filter priced := by simp [hxE, hp]
    simp [hm, hp]
  · have hm : ¬ x ∈ sold.eraseDups.filter priced := by simp [hp]
    simp [hm, hp]

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.PerBlockDistinctPriceLookup

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.PerBlockDistinctPriceLookup.sound
