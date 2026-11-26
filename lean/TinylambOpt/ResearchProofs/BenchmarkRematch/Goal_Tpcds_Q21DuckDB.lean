import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q21DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem sum_below (rows : List (Int × Int)) (cut : Int) :
    (rows.map (fun (d,q) => if d < cut then q else 0)).sum =
      ((rows.filter (fun r => r.1 < cut)).map Prod.snd).sum := by
  induction rows with
  | nil => rfl
  | cons r l ih =>
    cases r with
    | mk d q =>
      rw [List.map_cons, List.sum_cons, ih]
      dsimp only
      by_cases hc : d < cut
      · rw [ite_eq_left hc]
        rw [List.filter_cons_of_pos (p := fun r : Int × Int => r.1 < cut) (by simpa using hc)]
        rw [List.map_cons, List.sum_cons]
      · rw [ite_eq_right hc]
        rw [List.filter_cons_of_neg (p := fun r : Int × Int => r.1 < cut) (by simpa using hc)]
        rw [Int.zero_add]

private theorem sum_above (rows : List (Int × Int)) (cut : Int) :
    (rows.map (fun (d,q) => if d < cut then 0 else q)).sum =
      ((rows.filter (fun r => cut ≤ r.1)).map Prod.snd).sum := by
  induction rows with
  | nil => rfl
  | cons r l ih =>
    cases r with
    | mk d q =>
      rw [List.map_cons, List.sum_cons, ih]
      dsimp only
      by_cases hc : d < cut
      · rw [ite_eq_left hc]
        rw [List.filter_cons_of_neg (p := fun r : Int × Int => cut ≤ r.1) (by simpa using hc)]
        rw [Int.zero_add]
      · rw [ite_eq_right hc]
        rw [List.filter_cons_of_pos (p := fun r : Int × Int => cut ≤ r.1)
          (by simpa using (show cut ≤ d from by omega))]
        rw [List.map_cons, List.sum_cons]

theorem sound : Q21DuckDB.Goal := by
  intro rows cut
  simp only [total]
  rw [sum_below, sum_above]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q21DuckDB
