import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_PermutedLiteralGuards

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA

theorem sound : TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.PermutedLiteralGuards.Goal := by
  intro env e i j x y hij
  have hcomm : specialize i x (specialize j y e) = specialize j y (specialize i x e) := by
    induction e with
    | arg k =>
        by_cases hki : k == i <;> by_cases hkj : k == j
        · simp at hki hkj
          exact (hij (hki.symm.trans hkj)).elim
        · simp [specialize, hki, hkj]
        · simp [specialize, hki, hkj]
        · simp [specialize, hki, hkj]
    | lit v => rfl
    | neg a ih => simp [specialize, ih]
    | isTrue a ih => simp [specialize, ih]
    | missing a ih => simp [specialize, ih]
    | conj a b iha ihb => simp [specialize, iha, ihb]
    | disj a b iha ihb => simp [specialize, iha, ihb]
    | co a b iha ihb => simp [specialize, iha, ihb]
    | branch p a b ihp iha ihb => simp [specialize, ihp, iha, ihb]
  rw [hcomm]

end TinylambOpt.ResearchProofs.JitKernels.Goal_MaskSpecialization_PermutedLiteralGuards
