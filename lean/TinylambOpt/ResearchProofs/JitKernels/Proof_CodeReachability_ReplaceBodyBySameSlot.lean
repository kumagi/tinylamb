import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# ReplaceBodyBySameSlot (refuted)

Claim: if two generations `(slot,g)` and `(slot,h)` (`g < h`) are both present
in the heap, an old direct call may be re-pointed at the newest generation in
the same address slot without relinking, because the two objects would have the
same outgoing call table.

Refutation: generations in one slot are different compilations and may call
different targets.  Counterexample: the heap contains `(0,1)` calling `(1,1)`
and `(0,2)` calling `(2,2)`.  Both keys are in `heap.map Obj.key` and `1 < 2`,
but `edges heap (0,1) = [(1,1)] ≠ [(2,2)] = edges heap (0,2)`. -/

namespace TinylambOpt.ResearchProofs.JitKernels.CodeReachability_ReplaceBodyBySameSlot
open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

private def heap : List Obj := [⟨(0,1), [(1,1)], 0⟩, ⟨(0,2), [(2,2)], 0⟩]

theorem refute : ¬ReplaceBodyBySameSlot.Goal := by
  intro h
  have hd : edges heap (0,1) ≠ edges heap (0,2) := by
    show [(1,1)] ≠ [(2,2)]
    decide
  exact hd (h heap 0 1 2 (by decide) (by decide) (by decide))

end TinylambOpt.ResearchProofs.JitKernels.CodeReachability_ReplaceBodyBySameSlot
