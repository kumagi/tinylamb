import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `AntiPredicate.LimitBeforeFault` — LIMIT 1 must choose the earliest of a qualifying
  lane and a failing lane.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_AntiPredicate_LimitBeforeFault

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem sound : AntiPredicate.LimitBeforeFault.Goal := by
  apply Common.limitBeforeFault_of AntiPredicate.test
  exact Lanes.antipredicate_yes
  exact Lanes.antipredicate_err

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_AntiPredicate_LimitBeforeFault
