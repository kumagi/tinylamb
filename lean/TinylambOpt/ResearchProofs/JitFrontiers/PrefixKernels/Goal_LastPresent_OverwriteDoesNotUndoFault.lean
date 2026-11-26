import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_LastPresent_OverwriteDoesNotUndoFault

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : LastPresent.OverwriteDoesNotUndoFault.Goal := by
  intro w
  have hbad : fits w (limit w) = false := not_fits_limit w
  unfold execute
  simp [LastPresent.update, apply, hbad]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_LastPresent_OverwriteDoesNotUndoFault
