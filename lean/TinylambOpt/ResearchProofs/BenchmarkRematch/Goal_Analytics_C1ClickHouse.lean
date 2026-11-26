import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_C1ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group

theorem sound : C1ClickHouse.Goal := by
  intro posts comments votes
  dsimp only
  induction posts with
  | nil => simp
  | cons k ps ih =>
    by_cases h : comments.contains k = true
    · simp only [List.filter_cons, h, ↓reduceIte, List.map_cons, List.sum_cons, ih]
    · have h0 : comments.count k = 0 := by
        rw [List.count_eq_zero]
        intro hk
        exact h (List.contains_iff_mem.mpr hk)
      simp only [List.filter_cons, h, Bool.false_eq_true, ↓reduceIte, List.map_cons, List.sum_cons, ih, h0,
        Nat.zero_mul, Nat.zero_add]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_C1ClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_C1ClickHouse.sound
