import TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

/-!
A larger modulus does not imply that it is a multiple of the smaller one. A
value can satisfy the coarse stride-7 class while failing an unrelated stride-6
residue test, even though both intervals contain it.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_IntersectionByLargerStride

open TinylambOpt.ResearchGoals.JitKernels.GuardCertificates

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.GuardCertificates.IntersectionByLargerStride.Goal := by
  intro h
  have h' := h
    ({ lo := 0, hi := 100, modulus := 6, residue := 6 } : Domain)
    ({ lo := 0, hi := 100, modulus := 7, residue := 0 } : Domain) 7
    (by native_decide) (by native_decide) (by native_decide)
    (by unfold inside; native_decide) (by native_decide) (by native_decide)
  have hne : ¬ inside ({ lo := 0, hi := 100, modulus := 6, residue := 6 } : Domain) 7 := by
    unfold inside
    native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_GuardCertificates_IntersectionByLargerStride
