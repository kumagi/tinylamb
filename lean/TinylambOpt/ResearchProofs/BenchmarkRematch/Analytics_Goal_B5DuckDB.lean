import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B5DuckDB

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

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.B5DuckDB.Goal := by
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

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B5DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B5DuckDB.sound
