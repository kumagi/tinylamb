import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_C1ClickHouse

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.C1ClickHouse.Goal := by
  intro posts comments votes
  dsimp only
  induction posts with
  | nil => simp
  | cons k t ih =>
      by_cases hc : k ∈ comments
      · simp [hc, ih]
      · have hcnt : comments.count k = 0 := (List.count_eq_zero).2 hc
        simp [hc, hcnt, ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_C1ClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_C1ClickHouse.sound
