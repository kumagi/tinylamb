import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

/-! Two generations in the same address slot need not share a call table. A
direct call compiled against an older generation cannot be redirected to the
newer body without relinking its dependencies. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_ReplaceBodyBySameSlot

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

theorem refute : ¬ ReplaceBodyBySameSlot.Goal := by
  intro h
  let heap : List Obj := [
    { key := (7, 0), calls := [(1, 0)], bytes := 0 },
    { key := (7, 1), calls := [(2, 0)], bytes := 0 }]
  have bad : edges heap (7, 0) = edges heap (7, 1) :=
    h heap 7 0 1 (by native_decide) (by native_decide) (by native_decide)
  exact (by native_decide : ¬ edges heap (7, 0) = edges heap (7, 1)) bad

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_ReplaceBodyBySameSlot
