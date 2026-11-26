import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q29DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem sum_filter_ite :
    ∀ (l : List (Nat × Int)) (f : Nat × Int → Int) (p : Nat × Int → Bool),
      ((l.filter p).map f).sum = (l.map (fun x => if p x = true then f x else 0)).sum := by
  intro l f p
  induction l with
  | nil => rfl
  | cons a t ih =>
    by_cases hp : p a
    · rw [List.filter_cons_of_pos hp, List.map_cons, List.sum_cons, ih,
        List.map_cons, List.sum_cons]
      simp [hp]
    · rw [List.filter_cons_of_neg hp, ih, List.map_cons, List.sum_cons]
      rw [ite_eq_right hp, Int.zero_add]

theorem sound : Q29DuckDB.Goal := by
  intro catalog tickets
  simp only [total]
  rw [sum_filter_ite]
  refine congrArg List.sum (congrArg (fun g : Nat × Int → Int => List.map g catalog) ?_)
  funext x
  cases x with
  | mk k q =>
    by_cases hz : tickets k = 0 <;> simp [hz]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q29DuckDB
