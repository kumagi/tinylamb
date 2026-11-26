import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedMagnitude_NonnegativeRun

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : SignedMagnitude.NonnegativeRun.Goal := by
  intro rs h
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      rw [whole_cons]
      have hrest : ∀ x ∈ rs.filterMap id, 0 ≤ x := by
        intro x hx
        apply h x
        cases r with
        | none => simpa using hx
        | some y => simp; exact Or.inr (by simpa using hx)
      have ih' := ih hrest
      rw [ih']
      cases r with
      | none =>
          simp [SignedMagnitude.update, compose_one]
      | some x =>
          have hx : 0 ≤ x := h x (by simp)
          have hxabs : (x.natAbs : Int) = x := Int.natAbs_of_nonneg hx
          have hnot : ¬ x < 0 := by omega
          simp [SignedMagnitude.update, compose, hxabs, hnot]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedMagnitude_NonnegativeRun
