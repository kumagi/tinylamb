import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

/-! A chain of one-call bodies can be longer than one callee per root. The
one-extra-callee-per-root budget therefore undercharges reachable code and
cannot bound reclamation by root count alone. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_StubOnlySpaceBudget

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

theorem refute : ¬ StubOnlySpaceBudget.Goal := by
  intro h
  let heap : List Obj := [
    { key := (0, 0), calls := [(0, 1)], bytes := 1 },
    { key := (0, 1), calls := [(0, 2)], bytes := 1 },
    { key := (0, 2), calls := [], bytes := 1 }]
  let roots : List Key := [(0, 0)]
  have bad := h heap roots (by native_decide) (by native_decide)
  have hleft : ((retain heap (mark heap roots heap.length)).map Obj.bytes).sum = 3 := by
    native_decide
  have hright : ((retain heap roots).map Obj.bytes).sum + roots.length * ((heap.map Obj.bytes).max?.getD 0) = 2 := by
    native_decide
  rw [hleft, hright] at bad
  omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_StubOnlySpaceBudget
