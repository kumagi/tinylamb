import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q27_TimeTieBoundaryForSecondaryStringSortGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

theorem sound : Q27_TimeTieBoundaryForSecondaryStringSortGoal := by
  intro rows tau
  dsimp only
  intro r hr hgt hmem
  have hle : r.1 ≤ tau := by simpa using (List.mem_filter.mp hmem).2
  omega

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q27_TimeTieBoundaryForSecondaryStringSortGoal
