import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ZeroPriorSalesGroupEarlyAbandon

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# ZeroPriorSalesGroupEarlyAbandon

A certified upper bound on every unread payment, together with a non-positive
bound on the sum of the seen payments and the worst case for the pending ones,
rules out a positive total. The executor can abandon the group without reading
the remaining measures.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

/-! A list whose entries are bounded above by `cap` has sum at most its length
times `cap`; the cast to Int is the exact row count. -/
theorem sum_le_length_mul_cap (xs : List Int) (cap : Int)
    (h : ∀ x ∈ xs, x ≤ cap) :
    xs.sum ≤ (xs.length : Int) * cap := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      have hx : x ≤ cap := h x (by simp)
      have hxs : ∀ y ∈ xs, y ≤ cap := by intro y hy; exact h y (by simp [hy])
      have hi := ih hxs
      simp only [List.sum_cons]
      have hlen : ((x :: xs).length : Int) = (xs.length : Int) + 1 := by simp
      rw [hlen, Int.add_mul, Int.one_mul]
      omega

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.ZeroPriorSalesGroupEarlyAbandon.Goal := by
  intro seen pending cap hcap hbound hpos
  have hp := sum_le_length_mul_cap pending cap hcap
  rw [List.sum_append] at hpos
  omega
end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ZeroPriorSalesGroupEarlyAbandon
