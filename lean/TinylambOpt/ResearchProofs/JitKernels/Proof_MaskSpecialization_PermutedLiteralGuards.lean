import TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization

/-! Independent proof record for the MaskSpecialization.PermutedLiteralGuards research goal: the proposed rewrite is sound and preserves the modeled result. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_MaskSpecialization_PermutedLiteralGuards
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization.PermutedLiteralGuards
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.MaskSpecialization
open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA


theorem specialize_commute (i j : Nat) (x y : V) (hij : i ≠ j) (e : E) :
    specialize i x (specialize j y e) = specialize j y (specialize i x e) := by
  induction e with
  | arg k =>
      by_cases hki : k == i
      · have hkj : (k == j) = false := by
          have hki' : k = i := by simpa using hki
          subst k
          simp [hij]
        simp [specialize, hki, hkj]
      · by_cases hkj : k == j
        · simp [specialize, hki, hkj]
        · simp [specialize, hki, hkj]
  | lit w => simp [specialize]
  | neg a ih => simp [specialize, ih]
  | isTrue a ih => simp [specialize, ih]
  | missing a ih => simp [specialize, ih]
  | conj a b iha ihb => simp [specialize, iha, ihb]
  | disj a b iha ihb => simp [specialize, iha, ihb]
  | co a b iha ihb => simp [specialize, iha, ihb]
  | branch p a b ihp iha ihb => simp [specialize, ihp, iha, ihb]

theorem sound : Goal := by
  intro env e i j x y hij
  exact congrArg (run env) (specialize_commute i j x y hij e)

end Nov26.Proof_MaskSpecialization_PermutedLiteralGuards
