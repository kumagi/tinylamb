import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! Clearing the bits outside a guard's mask from its value leaves acceptance unchanged, so a specialization cache key can ignore those bits. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_CanonicalGuardCodeKey

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

theorem sound : TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CanonicalGuardCodeKey.Goal := by
  intro e w
  unfold accepts
  have h : (e.value &&& e.mask) &&& e.mask = e.value &&& e.mask := by
    ext i
    simp
  rw [h]

end TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_CanonicalGuardCodeKey
