import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `AntiPredicate.NullOutput` — A projection must preserve UNKNOWN, even when WHERE
  would discard it.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_AntiPredicate_NullOutput

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem sound : AntiPredicate.NullOutput.Goal := by
  apply Common.nullOutput_of AntiPredicate.test
  exact Lanes.antipredicate_nul

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_AntiPredicate_NullOutput
