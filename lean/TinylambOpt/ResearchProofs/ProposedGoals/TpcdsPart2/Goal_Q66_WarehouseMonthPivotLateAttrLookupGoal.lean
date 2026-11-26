import TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2

/-!
# Late warehouse attribute lookup after month pivot (TPC-DS Q66)

Rows selected by `(warehouse_sk, month)` all carry the same warehouse key, so projecting
`attr r.1` is identical to projecting `attr w` after the fact-table aggregation. The
executor may defer the wide warehouse attribute lookup to unique keys after pivoting.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q66_WarehouseMonthPivotLateAttrLookupGoal

open TinylambOpt.ProposedGoals.BenchmarkRematch.TpcdsPart2
open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : Q66_WarehouseMonthPivotLateAttrLookupGoal := by
  intro facts w m attr
  let p := fun r : Nat × Nat × Int => (r.1 == w) && (r.2.1 == m)
  have hw : ∀ r ∈ facts.filter p, r.1 = w := by
    intro r hr
    have hp := List.mem_filter.mp hr
    have hbeq : (r.1 == w) = true := (Bool.and_eq_true_iff.mp hp.2).1
    exact eq_of_beq hbeq
  have h := List.map_congr_left (l := facts.filter p)
    (f := fun r : Nat × Nat × Int => (attr r.1, r.2.2))
    (g := fun r : Nat × Nat × Int => (attr w, r.2.2))
    (by intro r hr; rw [hw r hr])
  simpa only [p] using h

end TinylambOpt.ResearchProofs.ProposedGoals.TpcdsPart2.Goal_Q66_WarehouseMonthPivotLateAttrLookupGoal
