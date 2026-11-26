import TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds

set_option maxHeartbeats 0
namespace Proof_Q40DuckDB
open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem getDsum : ∀ rs : List (Option Int), total rs (fun r => r.getD 0) = (rs.filterMap id).sum := by
  intro rs
  induction rs with
  | nil => simp [total]
  | cons a rs ih =>
    cases a with
    | none => simpa [total] using ih
    | some x =>
      simp [total] at ih ⊢
      exact ih

theorem subsum (price : Int) : ∀ rs : List (Option Int),
    total rs (fun r => price - r.getD 0) = (rs.length : Int) * price - total rs (fun r => r.getD 0) := by
  intro rs
  induction rs with
  | nil => simp [total]
  | cons a rs ih =>
    simp [total] at ih ⊢
    cases a with
    | none =>
      rw [ih]
      rw [Int.add_mul, Int.one_mul]
      omega
    | some x =>
      rw [ih]
      rw [Int.add_mul, Int.one_mul]
      omega

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Tpcds.Q40DuckDB.Goal := by
  intro price refunds
  by_cases h : (refunds.isEmpty) = true
  · have hn : refunds = [] := List.isEmpty_iff.mp h
    subst refunds
    rw [Int.max_eq_left (by decide)]
    simp [total]
  · have hne : refunds ≠ [] := by
      intro hz
      apply h
      rw [hz]
      rfl
    have hposNat : 1 ≤ refunds.length := by
      cases refunds with
      | nil => contradiction
      | cons a rs => simp
    have hmax : max (1 : Int) (refunds.length : Int) = (refunds.length : Int) := by
      apply Int.max_eq_right
      omega
    rw [ite_eq_right h, subsum, getDsum, hmax]

end Proof_Q40DuckDB

#print axioms Proof_Q40DuckDB.sound
