import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement
set_option linter.unusedSectionVars false

/-! Proof/counterexample for `TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.OutOfOrderLimitCredit.Goal`. The theorem below certifies the resource-policy claim (or refutes the strong hypothesis) in the model defined by that module, preserving the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
A row count from arbitrary finished morsels does NOT justify cancelling an
ordered LIMIT.

Claim (`OutOfOrderLimitCredit.Goal`): if every report in `rs` lies inside the
window and `k` rows were produced by `rs` overall, then the first `k` emitted rows
with `future` appended already match those from `rs` alone.

Refuted: rows produced by a morsel whose id is not contiguous with the frontier
contribute nothing to ordered output. Counterexample: `rs = [(1,[9])]`, window
`start = 0`, `count = 2`, `k = 1`, `future = [(0,[])]`. Id 1 did report 1 row, so
`k ≤ 1` passes, but id 0 was missing at `rs` time: `emit rs 0 2 = []` while after
id 0 completes with zero rows the frontier advances and
`emit (rs++future) 0 2 = [9]`. Cancelling on non-contiguous credit would drop the
output the frontier already made available.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_OutOfOrderLimitCredit
open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

theorem refute : ¬ OutOfOrderLimitCredit.Goal := by
  intro h
  have hc : consistent ([(1, [9])] ++ [(0, [])]) := by
    intro a ha b hb hab
    simp at ha hb
    rcases ha with rfl | rfl <;> rcases hb with rfl | rfl <;> simp_all
  exact absurd
    (h [(1, [9])] 0 2 1 [(0, [])] hc (by decide) (by decide)) (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_MorselRetirement_OutOfOrderLimitCredit
