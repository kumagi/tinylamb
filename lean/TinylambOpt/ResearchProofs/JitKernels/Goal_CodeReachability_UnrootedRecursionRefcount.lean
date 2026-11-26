import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

/-! An object can appear as a direct-call target without being reachable from
the roots: its caller may itself be dead. Reference counting by appearance in
call tables would therefore keep unreachable code alive. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_UnrootedRecursionRefcount

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

theorem refute : ¬ UnrootedRecursionRefcount.Goal := by
  intro h
  let heap : List Obj := [
    { key := (0, 0), calls := [], bytes := 0 },
    { key := (0, 1), calls := [(0, 2)], bytes := 0 },
    { key := (0, 2), calls := [], bytes := 0 }]
  let o : Obj := { key := (0, 2), calls := [], bytes := 0 }
  have bad := h heap [(0, 0)] o (by native_decide)
  have hnot : (0, 2) ∉ mark heap [(0, 0)] heap.length := by native_decide
  have hrhs : (0, 2) ∈ ([(0, 0)] : List Key) ∨ (0, 2) ∈ heap.flatMap Obj.calls := by
    native_decide
  exact hnot (bad.mpr hrhs)

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_UnrootedRecursionRefcount
