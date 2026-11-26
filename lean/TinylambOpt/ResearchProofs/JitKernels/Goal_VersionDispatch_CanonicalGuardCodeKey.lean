import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! Clearing the bits outside a guard's mask from its value leaves acceptance unchanged, so a specialization cache key can ignore those bits. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_CanonicalGuardCodeKey

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CanonicalGuardCodeKey

theorem sound : CanonicalGuardCodeKey.Goal := by
  intro e w
  simp [accepts]
  bv_decide

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_CanonicalGuardCodeKey
