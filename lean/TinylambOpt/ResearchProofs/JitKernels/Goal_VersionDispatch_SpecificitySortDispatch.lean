import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! Weaker does not require equal codes, so equally weak guards with different bodies are order-sensitive. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_SpecificitySortDispatch

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.SpecificitySortDispatch

theorem refute : ¬ SpecificitySortDispatch.Goal := by
  intro h
  let a : Entry := { mask := 0, value := 0, code := 0 }
  let b : Entry := { mask := 0, value := 0, code := 1 }
  have hw : weaker a b := by simp [weaker, clash, a, b]
  have h' := h a b 0 hw
  simp [dispatch, accepts, a, b] at h'

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_SpecificitySortDispatch
