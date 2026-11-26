import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q42DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Click.Q42DuckDB.Goal := by
  intro dims rank h
  have hdrop : ((dims.eraseDups.map rank).mergeSort (fun a b => decide (a ≤ b))).drop 10000 = [] := by
    rw [List.drop_eq_nil_iff]
    simpa using h
  simp only [TinylambOpt.ResearchGoals.BenchmarkRematch.ranges]
  rw [hdrop]
  simp

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click.Goal_Q42DuckDB
