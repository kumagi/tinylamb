import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `ConditionalNull.LimitBeforeFault` — LIMIT 1 must choose the earliest of a qualifying
  lane and a failing lane.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ConditionalNull_LimitBeforeFault

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem sound : ConditionalNull.LimitBeforeFault.Goal := by
  apply Common.limitBeforeFault_of ConditionalNull.test
  exact Lanes.conditionalnull_yes
  exact Lanes.conditionalnull_err

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_ConditionalNull_LimitBeforeFault
