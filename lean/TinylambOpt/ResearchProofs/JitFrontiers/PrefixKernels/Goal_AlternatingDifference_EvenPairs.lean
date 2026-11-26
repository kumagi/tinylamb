import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `AlternatingDifference.EvenPairs` — `∀ a b, whole update [some a,some b]=(1,b-a)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_AlternatingDifference_EvenPairs

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : AlternatingDifference.EvenPairs.Goal := by
  intro a b
  rw [whole_cons, whole_cons]
  simp [whole, AlternatingDifference.update, compose]
  omega


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_AlternatingDifference_EvenPairs
