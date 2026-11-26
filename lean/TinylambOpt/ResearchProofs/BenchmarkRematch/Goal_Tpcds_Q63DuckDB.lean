import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q63DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem sum_flatten_filterMap (blocks : List (List (Option Int))) :
    (blocks.flatten.filterMap id).sum = (blocks.map (fun b => (b.filterMap id).sum)).sum := by
  induction blocks with
  | nil => rfl
  | cons b rest ih =>
    rw [List.flatten_cons, List.filterMap_append, List.sum_append,
        List.map_cons, List.sum_cons, ih]

private theorem length_flatten_filterMap (blocks : List (List (Option Int))) :
    (blocks.flatten.filterMap id).length =
      (blocks.map (fun b => (b.filterMap id).length)).sum := by
  induction blocks with
  | nil => rfl
  | cons b rest ih =>
    rw [List.flatten_cons, List.filterMap_append, List.length_append,
        List.map_cons, List.sum_cons, ih]

theorem sound : Q63DuckDB.Goal := fun blocks =>
  Prod.ext (sum_flatten_filterMap blocks) (length_flatten_filterMap blocks)

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q63DuckDB.sound
