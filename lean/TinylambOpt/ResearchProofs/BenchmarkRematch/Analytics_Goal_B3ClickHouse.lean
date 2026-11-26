import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B3ClickHouse

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.B3ClickHouse.Goal := by
  intro sales dimRows eligible
  dsimp only
  induction sales with
  | nil => simp
  | cons k ks ih =>
      by_cases h : eligible k > 0
      · simp [h, ih]
      · have hz : eligible k = 0 := by omega
        simp [h, hz, ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B3ClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_B3ClickHouse.sound
