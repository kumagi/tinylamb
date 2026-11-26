import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q99
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q99.Goal := by
  intro runs bin
  dsimp only
  simp only [countWhere]
  rw [← List.countP_eq_length_filter]
  rw [List.countP_flatMap]
  induction runs with
  | nil => simp
  | cons r rs ih =>
    simp only [Function.comp_apply, List.map_cons, List.sum_cons]
    rw [List.countP_replicate]
    by_cases h : bucket [30, 60, 90, 120] (r.1.2 - r.1.1) == bin
    · simp [h, ih]
    · simp [h, ih]
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q99
