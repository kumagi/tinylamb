import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DecimalAppend_LeadingZeros

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : DecimalAppend.LeadingZeros.Goal := by
  intro w n rs
  change (if fits w (apply (DecimalAppend.update (some 0)) 0) then
            execute DecimalAppend.update w (n + 1) (apply (DecimalAppend.update (some 0)) 0) rs
          else .error n) = execute DecimalAppend.update w (n + 1) 0 rs
  simp [DecimalAppend.update, apply, fits_zero]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DecimalAppend_LeadingZeros
