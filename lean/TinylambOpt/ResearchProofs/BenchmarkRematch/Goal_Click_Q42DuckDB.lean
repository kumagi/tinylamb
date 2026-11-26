import TinylambOpt.ResearchGoals.BenchmarkRematch.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q42DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch.Click

theorem sound : Q42DuckDB.Goal := by
  intro dims rank h
  have hlen : (dims.eraseDups.map rank).mergeSort.length ≤ 10000 := by
    simp [h]
  have hdrop : (dims.eraseDups.map rank).mergeSort.drop 10000 = [] :=
    List.drop_eq_nil_iff.mpr hlen
  show ((dims.eraseDups.map rank).mergeSort.drop 10000).take 10 = []
  rw [hdrop]; simp

end TinylambOpt.ResearchProofs.BenchmarkRematch.Click_Q42DuckDB
