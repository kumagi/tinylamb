import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q66DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem flatten_total {α : Type} (ps : List (List α)) (f : α → Int) :
    total ps.flatten f = total ps (fun p => total p f) := by
  dsimp only [total]
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.flatten_cons, List.map_append, List.sum_append, List.map_cons, List.sum_cons]
      rw [ih]

theorem total_if_eq_filter {α : Type} [BEq α] [LawfulBEq α]
    (p : List (α × Int)) (month : α) :
    total p (fun r => if r.1 == month then r.2 else 0) =
      total (p.filter (fun r => r.1 == month)) Prod.snd := by
  induction p with
  | nil => simp [total]
  | cons r rs ih =>
      by_cases h : r.1 == month
      · simpa [total, h] using ih
      · simpa [total, h] using ih

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q66DuckDB.Goal := by
  intro pages month
  dsimp only
  rw [flatten_total]
  simp only [total]
  apply congrArg List.sum
  apply List.map_congr_left
  intro p hp
  exact total_if_eq_filter p month

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q66DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q66DuckDB.sound
