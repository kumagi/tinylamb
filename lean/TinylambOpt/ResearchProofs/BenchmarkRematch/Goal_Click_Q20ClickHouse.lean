import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q20ClickHouse
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : Q20ClickHouse.Goal := by
  intro xs k
  exact (List.filter_beq k).symm

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q20ClickHouse
