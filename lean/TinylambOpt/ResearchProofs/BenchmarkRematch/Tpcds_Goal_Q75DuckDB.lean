import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q75DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

theorem sum_sub (q : Int) : ∀ rs : List Int,
    total rs (fun v => q - v) = (rs.length : Int) * q - rs.sum := by
  intro rs
  induction rs with
  | nil => simp [total]
  | cons v t ih =>
      simp [total] at ih ⊢
      rw [ih]
      rw [Int.add_mul]
      simp
      omega

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q75DuckDB.Goal := by
  intro q rs
  cases rs with
  | nil =>
      simp
      rw [show max (1 : Int) 0 = 1 by decide]
      simp
  | cons v rs =>
      simp only [List.isEmpty_cons, Bool.false_eq_true, ite_false]
      rw [show max (1 : Int) ((v :: rs).length : Int) = ((v :: rs).length : Int) from
        Int.max_eq_right (by
          rw [show (1 : Int) = ((1 : Nat) : Int) by rfl]
          exact Int.ofNat_le.mpr (by simp : 1 ≤ (v :: rs).length))]
      exact sum_sub q (v :: rs)

end TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q75DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Tpcds.Goal_Q75DuckDB.sound
