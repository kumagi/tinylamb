import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ManagerMonthlyEqualTotalsFastReject

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# ManagerMonthlyEqualTotalsFastReject

When every non-null month equals the same value, its total is the length times that value
and every per-month deviation from the mean is zero. The positive-mean deviation filter
therefore keeps no month, so the executor may reject the whole group before evaluating
per-month work.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace MonthlyTotalHelpers

theorem sum_eq_length_mul (v : Int) : ∀ xs : List Int,
    (∀ x ∈ xs, x = v) → xs.sum = (xs.length : Int) * v := by
  intro xs
  induction xs with
  | nil => intro h; simp
  | cons x xs ih =>
      intro h
      have hx : x = v := h x (by simp)
      have ht : ∀ y ∈ xs, y = v := by
        intro y hy
        exact h y (by simp [hy])
      simp [hx, ih ht]
      rw [Int.add_mul, Int.one_mul]
      ac_rfl

end MonthlyTotalHelpers

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.ManagerMonthlyEqualTotalsFastReject.Goal := by
  intro months v h
  dsimp only
  let xs := months.filterMap id
  have hall : ∀ x ∈ xs, x = v := by
    intro x hx
    exact h x (by simpa [xs] using hx)
  have hsum : xs.sum = (xs.length : Int) * v := MonthlyTotalHelpers.sum_eq_length_mul v xs hall
  apply List.eq_nil_iff_forall_not_mem.mpr
  intro x hx
  have hxmem : x ∈ xs := (List.mem_filter.mp hx).1
  have hxtrue : decide (0 < xs.sum ∧ 10 * (x * (xs.length : Int) - xs.sum).natAbs > xs.sum) = true :=
    (List.mem_filter.mp hx).2
  have hxv : x = v := hall x hxmem
  have hdev : x * (xs.length : Int) - xs.sum = 0 := by
    rw [hsum, hxv]
    rw [Int.mul_comm v (xs.length : Int)]
    simp
  have hpredfalse :
      decide (0 < xs.sum ∧ 10 * (x * (xs.length : Int) - xs.sum).natAbs > xs.sum) = false := by
    rw [hdev]
    simp only [Int.natAbs_zero]
    by_cases hs : 0 < xs.sum
    · have hnot : ¬ (0 : Int) > xs.sum := by omega
      simp [hs, hnot]
    · simp [hs]
  rw [hpredfalse] at hxtrue
  cases hxtrue
end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ManagerMonthlyEqualTotalsFastReject
