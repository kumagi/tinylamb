import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q23
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Click

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q23.Goal := by
  intro pairs title url accepts allowed
  apply List.filter_congr
  intro x hx
  rw [Bool.eq_iff_iff]
  rw [List.elem_iff]
  rw [List.mem_filter]
  rw [List.mem_eraseDups]
  simp [hx]
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q23
