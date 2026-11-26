import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q25DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem const_map_sum : ∀ (c : List Int) (v : Int),
    (c.map (fun _ => v)).sum = (c.length : Int) * v := by
  intro c
  induction c with
  | nil => simp
  | cons x c ih =>
      intro v
      simp at ih ⊢
      rw [ih]
      rw [Int.add_mul]
      simp [Int.add_comm]

theorem one_sale : ∀ (returns catalog : List Int),
    (returns.flatMap (fun v => catalog.map (fun _ => v))).sum =
      returns.sum * (catalog.length : Int) := by
  intro returns
  induction returns with
  | nil => simp
  | cons v returns ih =>
      intro catalog
      simp at ih ⊢
      rw [ih, const_map_sum catalog v]
      rw [Int.add_mul]
      rw [Int.mul_comm (catalog.length : Int) v]

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q25DuckDB.Goal := by
  intro sales returns catalog
  induction sales with
  | nil => simp [total]
  | cons s sales ih =>
      simp [total] at ih ⊢
      rw [ih, one_sale]
      rw [Int.add_mul, Int.add_mul]
      simp [Int.add_comm]
