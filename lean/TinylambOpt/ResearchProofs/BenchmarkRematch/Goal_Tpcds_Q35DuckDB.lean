import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q35DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem filterMap_replicate_none (n : Nat) :
    (List.replicate n (none : Option Int)).filterMap id = [] := by
  induction n with
  | zero => rfl
  | succ m ih => rw [List.replicate_succ, List.filterMap_cons]; simp [ih]

private theorem filterMap_replicate_some (n : Nat) (x : Int) :
    (List.replicate n (some x)).filterMap id = List.replicate n x := by
  induction n with
  | zero => rfl
  | succ m ih => rw [List.replicate_succ, List.filterMap_cons]; simp [ih, List.replicate_succ]

theorem sound : Q35DuckDB.Goal := by
  intro v n hn
  cases v with
  | none =>
    rw [filterMap_replicate_none]
    exact rfl
  | some x =>
    rw [filterMap_replicate_some]
    dsimp only
    rw [List.min?_replicate_of_pos hn, List.max?_replicate_of_pos hn]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q35DuckDB
