import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-!
# NegativeFilterShortcutForSetDifference

`maybeSeen` is a no-false-negative filter for the exclusion tables: every key
of `c ++ w` is reported as maybe seen. Hence a definite miss reported by the
filter cannot occur in either exclusion table, and the negative shortcut
`!maybeSeen k || !(...)` agrees with the direct containment test. A caller may
skip reading both exclusion tables for keys the filter definitely rejects,
without changing EXCEPT DISTINCT output.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_NegativeFilterShortcutForSetDifference

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

theorem sound : NegativeFilterShortcutForSetDifference.Goal := by
  intro s c w maybeSeen hcover
  apply List.filter_congr
  intro k hk
  have hcont : (c ++ w).contains k = (c.contains k || w.contains k) := by
    simp
  rw [hcont]
  by_cases hm : maybeSeen k = true
  · simp [hm]
  · have hmfalse : maybeSeen k = false := Bool.eq_false_of_ne_true hm
    have hknot : k ∉ c ++ w := by
      intro hkin
      exact hm (hcover k hkin)
    have hcnot : k ∉ c := by
      intro hkc
      exact hknot (by simp [hkc])
    have hwnot : k ∉ w := by
      intro hkw
      exact hknot (by simp [hkw])
    simp [hmfalse, hcnot, hwnot]

end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.Goal_NegativeFilterShortcutForSetDifference
