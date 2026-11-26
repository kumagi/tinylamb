import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_NegativeFilterShortcutForSetDifference

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# NegativeFilterShortcutForSetDifference

A no-false-negative membership bitmap can certify that a key is outside both exclusion
tables only when it is missing from the bitmap; keys known to be in either table take the
other branch. The two filters therefore remove the same distinct keys. This lets EXCEPT
DISTINCT skip loading the exclusion tables for definite non-members while preserving the
result bag for every uncertain key.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace NegativeFilterHelpers

theorem contains_true {α : Type} [BEq α] [LawfulBEq α] (l : List α) (x : α) :
    l.contains x = true ↔ x ∈ l := by
  induction l with
  | nil => simp
  | cons y l ih =>
      by_cases h : y == x
      · have hyx : y = x := beq_iff_eq.mp h
        subst hyx
        simp
      · simp

theorem eq_false_of_not_true (b : Bool) (h : ¬ b = true) : b = false :=
  (Bool.not_eq_true b).mp h

end NegativeFilterHelpers

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.NegativeFilterShortcutForSetDifference.Goal := by
  intro s c w maybeSeen hcert
  apply List.filter_congr
  intro k hk
  by_cases hmem : k ∈ c ++ w
  · have hseen : maybeSeen k = true := hcert k hmem
    have hcw : (c.contains k || w.contains k) = true := by
      rcases List.mem_append.mp hmem with hc | hw
      · exact Bool.or_eq_true_iff.mpr
          (Or.inl ((NegativeFilterHelpers.contains_true c k).mpr hc))
      · exact Bool.or_eq_true_iff.mpr
          (Or.inr ((NegativeFilterHelpers.contains_true w k).mpr hw))
    have hcwlist : (c ++ w).contains k = true :=
      (NegativeFilterHelpers.contains_true (c ++ w) k).mpr hmem
    have hleft : (!maybeSeen k || !(c.contains k || w.contains k)) = false := by
      rw [hseen]
      simp only [Bool.not_true, Bool.false_or]
      rw [hcw]
      rfl
    have hright : (!(c ++ w).contains k) = false := by
      rw [hcwlist]
      rfl
    rw [hleft, hright]
  · have hcwlist : (c ++ w).contains k = false := by
      apply NegativeFilterHelpers.eq_false_of_not_true
      intro hb
      exact hmem ((NegativeFilterHelpers.contains_true (c ++ w) k).mp hb)
    have hcnotmem : k ∉ c := by
      intro hc
      exact hmem (List.mem_append.mpr (Or.inl hc))
    have hwnotmem : k ∉ w := by
      intro hw
      exact hmem (List.mem_append.mpr (Or.inr hw))
    have hcw : (c.contains k || w.contains k) = false := by
      apply NegativeFilterHelpers.eq_false_of_not_true
      intro hb
      rcases Bool.or_eq_true_iff.mp hb with hc' | hw'
      · exact hcnotmem ((NegativeFilterHelpers.contains_true c k).mp hc')
      · exact hwnotmem ((NegativeFilterHelpers.contains_true w k).mp hw')
    have hleft : (!maybeSeen k || !(c.contains k || w.contains k)) = true := by
      by_cases hseen : maybeSeen k = true
      · rw [hseen]
        simp only [Bool.not_true, Bool.false_or]
        rw [hcw]
        rfl
      · have hseenF : maybeSeen k = false :=
          NegativeFilterHelpers.eq_false_of_not_true _ hseen
        rw [hseenF]
        simp only [Bool.not_false, Bool.true_or]
    have hright : (!(c ++ w).contains k) = true := by
      rw [hcwlist]
      rfl
    rw [hleft, hright]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_NegativeFilterShortcutForSetDifference
