import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q25DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

private theorem sum_flatMap (xs : List Int) (g : Int → List Int) :
    (xs.flatMap g).sum = (xs.map (fun x => (g x).sum)).sum := by
  induction xs with
  | nil => rfl
  | cons x l ih =>
    rw [List.flatMap_cons, List.sum_append, ih, List.map_cons, List.sum_cons]

private theorem sum_map_const :
    ∀ (l : List Int) (v : Int), (l.map (fun _ => v)).sum = (l.length:Int) * v := by
  intro l
  induction l with
  | nil => intro v; simp
  | cons a l ih =>
    intro v
    rw [List.map_cons, List.sum_cons, ih v]
    simp only [List.length_cons, Int.natCast_add, Int.add_mul, Int.ofNat_one]
    omega

private theorem sum_map_mul_left :
    ∀ (l : List Int) (c : Int), (l.map (fun v => c * v)).sum = c * l.sum := by
  intro l
  induction l with
  | nil => intro c; simp
  | cons a l ih =>
    intro c
    rw [List.map_cons, List.sum_cons, ih c]
    rw [List.sum_cons, Int.mul_add]

theorem sound : Q25DuckDB.Goal := by
  intro sales returns catalog
  simp only [total, List.map_id]
  rw [sum_flatMap sales (fun _ => returns.flatMap (fun v => catalog.map (fun _ => v)))]
  rw [sum_flatMap returns (fun v => catalog.map (fun _ => v))]
  rw [show List.map (fun v => (List.map (fun _ => v) catalog).sum) returns =
        List.map (fun v => (catalog.length : Int) * v) returns from
      congrArg (fun g : Int → Int => List.map g returns) (funext fun v => sum_map_const catalog v)]
  rw [sum_map_mul_left returns (catalog.length : Int)]
  rw [sum_map_const sales ((catalog.length : Int) * returns.sum)]
  rw [Int.mul_comm ((catalog.length : Nat) : Int) returns.sum]
  rw [← Int.mul_assoc]

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds_Q25DuckDB
