import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

/-! A body called from an active frame is reachable even though it is not a
root. Treating active-frame entries as the only roots would reclaim live code
and change the targets of SQL execution. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_ActiveFramesAsOnlyRoots

open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

theorem refute : ¬ ActiveFramesAsOnlyRoots.Goal := by
  intro h
  have bad := h [{ key := (0, 0), calls := [(1, 0)], bytes := 0 }] [(0, 0)] (1, 0)
    (by native_decide)
  have hm : (1, 0) ∈ mark [{ key := (0, 0), calls := [(1, 0)], bytes := 0 }] [(0, 0)] 1 := by
    native_decide
  exact bad hm

end TinylambOpt.ResearchProofs.JitKernels.Goal_CodeReachability_ActiveFramesAsOnlyRoots
