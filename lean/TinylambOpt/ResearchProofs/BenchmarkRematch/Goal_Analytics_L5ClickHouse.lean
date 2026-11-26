import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L5ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group

theorem sound : L5ClickHouse.Goal := by
  intro edges people
  dsimp only
  induction edges with
  | nil => simp
  | cons k es ih =>
    by_cases h : people.eraseDups.contains k = true
    · simp only [List.filter_cons, h, ↓reduceIte, List.flatMap_cons, ih]
    · have hk : k ∉ people := by
        intro hk
        exact h (List.contains_iff_mem.mpr (List.mem_eraseDups.mpr hk))
      have h0 : people.filter (· == k) = [] := by
        rw [List.filter_eq_nil_iff]
        intro x hx
        simp
        intro e; subst e; exact hk hx
      simp only [List.filter_cons, h, Bool.false_eq_true, ↓reduceIte, List.flatMap_cons, ih, h0, List.nil_append]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L5ClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_L5ClickHouse.sound
