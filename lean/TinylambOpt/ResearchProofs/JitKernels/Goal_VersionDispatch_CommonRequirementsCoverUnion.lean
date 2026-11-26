import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! A word mixing values from two multi-bit guards can match neither even though it matches all common requirements. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_CommonRequirementsCoverUnion

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonRequirementsCoverUnion

theorem refute : ¬ CommonRequirementsCoverUnion.Goal := by
  intro h
  let a : Entry := { mask := 3, value := 0, code := 0 }
  let b : Entry := { mask := 3, value := 3, code := 0 }
  have h' := h a b 1 (by simp [a, b]) (by simp [a, b])
  simp [dispatch, accepts, a, b] at h'

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_CommonRequirementsCoverUnion
