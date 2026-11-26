import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `ExclusiveOr.MaskedTruth` — A ballot for TRUE equals scalar three-valued execution on
  just the active ordinals.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ExclusiveOr_MaskedTruth

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem sound : ExclusiveOr.MaskedTruth.Goal := by
  apply Common.maskedTruth_of ExclusiveOr.test
  exact Lanes.exclusiveor_yes

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ExclusiveOr_MaskedTruth
