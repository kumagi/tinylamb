import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_PositiveFilter_NonpositiveSkip

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : PositiveFilter.NonpositiveSkip.Goal := by
  intro x hx
  simp [PositiveFilter.update, Int.max_eq_left hx]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_PositiveFilter_NonpositiveSkip
