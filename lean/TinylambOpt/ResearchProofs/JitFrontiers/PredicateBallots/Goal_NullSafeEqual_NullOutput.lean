import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common_PredicateBallots
import TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes_PredicateBallots

/-!
Vector predicates use lane masks to carry the SQL truth or selection state into later
operators.

Proved

- `NullSafeEqual.NullOutput` — A projection must preserve UNKNOWN, even when WHERE
  would discard it.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_NullOutput

open TinylambOpt.ResearchGoals.JitFrontiers.PredicateBallots
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Common
open TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Lanes

theorem sound : NullSafeEqual.NullOutput.Goal := by
  apply Common.nullOutput_of NullSafeEqual.test
  exact Lanes.nullsafeequal_nul

end TinylambOpt.ResearchProofs.JitFrontiers.PredicateBallots.Goal_NullSafeEqual_NullOutput
