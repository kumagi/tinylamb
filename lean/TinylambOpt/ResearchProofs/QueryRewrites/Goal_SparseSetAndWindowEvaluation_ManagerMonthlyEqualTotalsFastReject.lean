import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-!
# ManagerMonthlyEqualTotalsFastReject

When every non-null monthly total is the certified constant `v`, the sum is
`length * v` and each deviation from the mean is exactly zero. The deviation
filter then asks for both a strictly positive total and a strictly negative
total, so no month can pass. SQL may reject such a group before evaluating the
per-month average predicate.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ManagerMonthlyEqualTotalsFastReject

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

private theorem sum_const {v : Int} : ∀ ys : List Int, (∀ x ∈ ys, x = v) →
    ys.sum = (ys.length : Int) * v := by
  intro ys
  induction ys with
  | nil => simp
  | cons x xt ih =>
      intro h
      have hx : x = v := h x (by simp)
      have ht : ∀ y ∈ xt, y = v := by
        intro y hy
        exact h y (by simp [hy])
      rw [List.sum_cons, ih ht, hx]
      simp [Int.add_mul, Int.one_mul, Int.add_comm]

theorem sound : ManagerMonthlyEqualTotalsFastReject.Goal := by
  intro months v hall
  let xs := months.filterMap id
  have hall' : ∀ x ∈ xs, x = v := by
    intro x hx
    exact hall x (by simpa [xs] using hx)
  have hsum : xs.sum = (xs.length : Int) * v := sum_const xs hall'
  apply List.eq_nil_iff_forall_not_mem.mpr
  intro x hx
  have hxv : x = v := hall' x (List.mem_filter.mp hx).1
  have hdiff : x * (xs.length : Int) - xs.sum = 0 := by
    rw [hsum, hxv, Int.mul_comm v (xs.length : Int)]
    exact Int.sub_self _
  have hnat : (x * (xs.length : Int) - xs.sum).natAbs = 0 := by
    rw [hdiff]
    simp
  have hpass : decide (0 < xs.sum ∧
      10 * (x * (xs.length : Int) - xs.sum).natAbs > xs.sum) = false := by
    simp [hnat]
    omega
  have hp : decide (0 < xs.sum ∧
      10 * (x * (xs.length : Int) - xs.sum).natAbs > xs.sum) = true :=
    (List.mem_filter.mp hx).2
  rw [hpass] at hp
  contradiction

end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ManagerMonthlyEqualTotalsFastReject
