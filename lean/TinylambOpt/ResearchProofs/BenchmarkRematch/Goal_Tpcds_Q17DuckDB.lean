import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q17DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem sum_replicate_sq (n : Nat) (x : Int) :
    ((List.replicate n x).map (fun v => v*v)).sum = (n:Int) * (x*x) := by
  induction n with
  | zero => simp
  | succ m ih =>
    rw [List.replicate_succ, List.map_cons, List.sum_cons, ih]
    simp only [Int.natCast_add, Int.ofNat_one, Int.add_mul, Int.one_mul]
    omega

private theorem sum_flatMap_map (xs : List Int) (n : Nat) :
    ((xs.flatMap (List.replicate n)).map (fun v => v*v)).sum =
      (xs.map (fun x => ((List.replicate n x).map (fun v => v*v)).sum)).sum := by
  induction xs with
  | nil => rfl
  | cons x l ih =>
    rw [List.flatMap_cons, List.map_append, List.sum_append, ih, List.map_cons, List.sum_cons]

private theorem sum_map_mul (xs : List Int) (n : Int) :
    (xs.map (fun x => n * (x*x))).sum = n * (xs.map (fun x => x*x)).sum := by
  induction xs with
  | nil => simp
  | cons x l ih =>
    rw [List.map_cons, List.sum_cons, List.map_cons, List.sum_cons, ih]
    simp only [Int.mul_add]

theorem sound : Q17DuckDB.Goal := by
  intro xs n
  simp only [total]
  rw [sum_flatMap_map]
  rw [show List.map (fun x => (List.map (fun v => v * v) (List.replicate n x)).sum) xs =
        List.map (fun x => (n : Int) * (x * x)) xs from
      congrArg (fun g : Int → Int => List.map g xs) (funext fun x => sum_replicate_sq n x)]
  rw [sum_map_mul]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q17DuckDB
