import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

/-! Two equal-body versions differing in exactly one masked bit can be replaced by one version with that bit unguarded. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_FoldNeighborVersions

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch
open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.FoldNeighborVersions

theorem sound : FoldNeighborVersions.Goal := by
  intro a b i w hmask hcode hbit hdiff
  by_cases ha : accepts a w
  · simp [dispatch, ha]
    simp [accepts] at ha ⊢
    bv_decide
  · by_cases hb : accepts b w
    · simp [dispatch, ha, hb, ← hcode]
      simp [accepts] at ha hb hmask hdiff ⊢
      bv_decide
    · simp [dispatch, ha, hb]
      simp [accepts] at ha hb hmask hdiff ⊢
      bv_decide

end TinylambOpt.ResearchProofs.JitKernels.VersionDispatch.Goal_VersionDispatch_FoldNeighborVersions
