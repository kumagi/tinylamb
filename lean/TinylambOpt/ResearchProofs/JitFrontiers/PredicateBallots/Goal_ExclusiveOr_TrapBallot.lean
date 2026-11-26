import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `ExclusiveOr.TrapBallot` — Mask exceptional RHS lanes by demand before selecting the
  first error.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ExclusiveOr_TrapBallot

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem sound : ExclusiveOr.TrapBallot.Goal := by
  apply Common.trapBallot_of ExclusiveOr.test
  exact Lanes.exclusiveor_err

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ExclusiveOr_TrapBallot
