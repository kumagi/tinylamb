import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `SignedMagnitude.JoinCertificates` — Shard summaries can validate both pieces without
  replaying the first.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedMagnitude_JoinCertificates

open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels

theorem sound : SignedMagnitude.JoinCertificates.Goal := by
  exact gen_joinCertificates SignedMagnitude.update

end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedMagnitude_JoinCertificates
