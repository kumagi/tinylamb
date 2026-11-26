import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q02ClickHouse

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem sound : Q02ClickHouse.Goal := by
  intro a b
  rw [List.length_flatMap]
  refine congrArg List.sum (congrArg (fun f : Nat → Nat => List.map f a) ?_)
  apply funext; intro w
  exact List.count_eq_length_filter.symm

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q02ClickHouse
