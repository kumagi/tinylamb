import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q32ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : Q32ClickHouse.Goal := by
  intro ids keep pair
  induction ids with
  | nil => rfl
  | cons x t ih =>
    cases h : keep x <;> simp [h, ih]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q32ClickHouse
