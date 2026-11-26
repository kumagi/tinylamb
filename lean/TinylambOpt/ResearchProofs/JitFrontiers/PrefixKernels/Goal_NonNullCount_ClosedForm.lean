import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_NonNullCount_ClosedForm

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : NonNullCount.ClosedForm.Goal := by
  intro rs
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      rw [whole_cons, ih]
      cases r with
      | none => simp [NonNullCount.update, compose_one]
      | some x =>
          simp [NonNullCount.update, compose_one]
          omega


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_NonNullCount_ClosedForm
