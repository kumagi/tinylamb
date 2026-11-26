import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

set_option linter.unusedSimpArgs false
set_option linter.deprecated false

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_SparseSetAndWindowEvaluation_ZeroPriorSalesGroupEarlyAbandon
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.ZeroPriorSalesGroupEarlyAbandon

theorem sound : Goal := by
  intro seen pending cap hcap hbound hpos
  have hpend : pending.sum ≤ (pending.length : Int) * cap := by
    have h : ∀ l : List Int, (∀ x ∈ l, x ≤ cap) → l.sum ≤ (l.length : Int) * cap := by
      intro l hl
      induction l with
      | nil => simp
      | cons a as ih =>
          have ha : a ≤ cap := hl a (by simp)
          have has : ∀ x ∈ as, x ≤ cap := fun x hx => hl x (by simp [hx])
          have ih' := ih has
          have hlen : ((a :: as).length : Int) = (as.length : Int) + 1 := by simp
          rw [List.sum_cons, hlen, Int.add_mul, Int.one_mul]
          omega
    exact h pending hcap
  rw [List.sum_append] at hpos
  omega
end Nov26.Proof_SparseSetAndWindowEvaluation_ZeroPriorSalesGroupEarlyAbandon
