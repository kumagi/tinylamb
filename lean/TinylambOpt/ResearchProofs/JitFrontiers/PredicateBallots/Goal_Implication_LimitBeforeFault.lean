import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `Implication.LimitBeforeFault` — LIMIT 1 must choose the earliest of a qualifying
  lane and a failing lane.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Implication_LimitBeforeFault

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem sound : Implication.LimitBeforeFault.Goal := by
  apply Common.limitBeforeFault_of Implication.test
  exact Lanes.implication_yes
  exact Lanes.implication_err

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_Implication_LimitBeforeFault
