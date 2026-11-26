import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q32ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q32ClickHouse.Goal := by
  intro ids keep pair
  induction ids with
  | nil => simp
  | cons i is ih =>
      by_cases h : keep i
      · simp [h, ih]
      · simp [h, ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q32ClickHouse
