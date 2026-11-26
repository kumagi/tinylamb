import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `DifferenceFromBase.DropMasked` — Compile masked lanes as identities and recover the
  selected result, given a valid incoming value.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DifferenceFromBase_DropMasked

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : DifferenceFromBase.DropMasked.Goal := by
  exact gen_dropMasked DifferenceFromBase.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DifferenceFromBase_DropMasked
