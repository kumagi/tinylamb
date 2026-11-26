import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q40DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem sum_map_const {α : Type} :
    ∀ (l : List α) (v : Int), (l.map (fun _ => v)).sum = (l.length:Int) * v := by
  intro l
  induction l with
  | nil => intro v; simp
  | cons a l ih =>
    intro v
    rw [List.map_cons, List.sum_cons, ih v]
    simp only [List.length_cons, Int.natCast_add, Int.add_mul, Int.ofNat_one]
    omega

private theorem helper (price : Int) :
    ∀ (os : List (Option Int)),
      (os.map (fun r => price - r.getD 0)).sum =
        (os.map (fun _ => price)).sum - (os.filterMap id).sum := by
  intro os
  induction os with
  | nil => rfl
  | cons r l ih =>
    rw [List.map_cons, List.sum_cons, ih, List.map_cons, List.sum_cons]
    cases r with
    | none =>
      rw [Option.getD_none,
        show List.filterMap id (none :: l) = List.filterMap id l from rfl,
        Int.sub_zero]
      omega
    | some v =>
      rw [Option.getD_some,
        show List.filterMap id (some v :: l) = v :: List.filterMap id l from rfl,
        List.sum_cons]
      omega

theorem sound : Q40DuckDB.Goal := by
  intro price refunds
  cases refunds with
  | nil =>
    have hm : (max (1 : Int) (([] : List (Option Int)).length : Int)) = 1 := by
      rw [List.length_nil]; rfl
    rw [hm]
    simp [total]
  | cons r rs =>
    rw [ite_eq_right (by simp)]
    simp only [total]
    rw [helper price (r :: rs), sum_map_const (r :: rs) price]
    have hm : (max (1 : Int) ((r :: rs).length : Int)) = ((r :: rs).length : Int) := by
      rw [List.length_cons, Int.natCast_add, Int.ofNat_one]
      omega
    rw [hm]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q40DuckDB
