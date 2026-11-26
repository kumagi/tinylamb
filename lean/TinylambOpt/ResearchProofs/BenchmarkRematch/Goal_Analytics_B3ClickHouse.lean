import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
import TinylambOpt.ResearchProofs.BenchmarkRematch.Group

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B3ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics
open TinylambOpt.ResearchProofs.BenchmarkRematch.Group

theorem sound : B3ClickHouse.Goal := by
  intro sales dimRows eligible
  dsimp only
  induction sales with
  | nil => simp
  | cons k ss ih =>
    by_cases h : eligible k > 0
    · simp [List.filter_cons, h, ih]
    · have h0 : eligible k = 0 := by omega
      simp [List.filter_cons, h, h0, ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B3ClickHouse

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Goal_Analytics_B3ClickHouse.sound
