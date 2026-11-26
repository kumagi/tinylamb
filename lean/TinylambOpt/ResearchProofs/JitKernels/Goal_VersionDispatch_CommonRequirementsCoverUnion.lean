import TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_CommonRequirementsCoverUnion

open TinylambOpt.ResearchGoals.JitKernels.VersionDispatch

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.VersionDispatch.CommonRequirementsCoverUnion.Goal := by
  intro h
  let a : Entry := ⟨3, 0, 7⟩
  let b : Entry := ⟨3, 3, 7⟩
  let w : Word := 1
  have hm : a.mask = b.mask := by rfl
  have hc : a.code = b.code := by rfl
  have hres := h a b w hm hc
  have hbad :
      dispatch [a, b] w ≠
        dispatch [{a with mask := a.mask &&& ~~~(a.value ^^^ b.value)}] w := by
    native_decide
  exact hbad hres

end TinylambOpt.ResearchProofs.JitKernels.Goal_VersionDispatch_CommonRequirementsCoverUnion
