import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-! Bits outside a guard mask are don't-cares, so re-masking the stored value is semantics-preserving. -/
namespace Nov26.Proof_VersionDispatch_CanonicalGuardCodeKey
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CanonicalGuardCodeKey
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CanonicalGuardCodeKey
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CanonicalGuardCodeKey
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CanonicalGuardCodeKey

theorem sound : Goal := by
  intro e w
  simp [accepts, BitVec.and_assoc, BitVec.and_self]

end Nov26.Proof_VersionDispatch_CanonicalGuardCodeKey