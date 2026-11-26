import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q49
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q49.Goal := by
  intro sold refunds
  dsimp
  induction sold with
  | nil => rfl
  | cons k ks ih =>
    simp only [List.flatMap_cons, List.filter_append]
    rw [ih]
    by_cases h : (refunds k).isEmpty
    · have hkempty : refunds k = [] := List.isEmpty_iff.mp h
      simp [h, hkempty]
    · simp [h]
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q49
