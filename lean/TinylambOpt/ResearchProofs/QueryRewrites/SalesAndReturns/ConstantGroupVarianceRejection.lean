import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# ConstantGroupVarianceRejection

An integer group whose minimum and maximum agree has a zero variance numerator.

A variance-based HAVING test can avoid squared-value accumulation for a group certified
constant, subject to the SQL test's NULL and denominator conditions.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ConstantGroupVarianceRejection
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sum_all_eq (xs : List Int) (v : Int) (h : ∀ x ∈ xs, x = v) :
    xs.sum = (xs.length : Int) * v := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    have hx : x = v := h x (by simp)
    have ht : ∀ y ∈ xs, y = v := by intro y hy; exact h y (by simp [hy])
    simp [hx, ih ht]
    rw [Int.add_mul, Int.one_mul, Int.add_comm]

theorem sumsq_all_eq (xs : List Int) (v : Int) (h : ∀ x ∈ xs, x = v) :
    (xs.map (fun x => x*x)).sum = (xs.length : Int) * (v*v) := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    have hx : x = v := h x (by simp)
    have ht : ∀ y ∈ xs, y = v := by intro y hy; exact h y (by simp [hy])
    simp [hx, ih ht]
    rw [Int.add_mul, Int.one_mul, Int.add_comm]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.ConstantGroupVarianceRejection.Goal := by
  intro xs v hmin hmax
  have hle : ∀ x ∈ xs, v ≤ x := (List.min?_eq_some_iff.mp hmin).2
  have hge : ∀ x ∈ xs, x ≤ v := (List.max?_eq_some_iff.mp hmax).2
  have hall : ∀ x ∈ xs, x = v := by
    intro x hx
    exact Int.le_antisymm (hge x hx) (hle x hx)
  unfold total
  rw [sumsq_all_eq xs v hall, sum_all_eq xs v hall]
  rw [show (xs.length : Int) * v * ((xs.length : Int) * v) =
      (xs.length : Int) * ((xs.length : Int) * (v * v)) by ac_rfl]
  exact Int.sub_self _
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.ConstantGroupVarianceRejection
