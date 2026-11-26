import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

namespace TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_NonNullOnlySpillGroupPresence

open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay


/-! Scratch proof: NULL-only groups are lost by sparse spill filtering. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

/-- A group whose only input is NULL still owns a key; a sparse flush that
    drops such rows loses the group key entirely. One row with key 0 and a
    NULL payload is a concrete counterexample. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay.NonNullOnlySpillGroupPresence.Goal := by
  intro h
  have hbad := h ([(0, (none : Option Int))] : Rows)
  exact (by decide : ¬ ((([(0, (none : Option Int))] : Rows).filter (fun r => r.2.isSome)).map Prod.fst).eraseDups =
    ((([(0, (none : Option Int))] : Rows).map Prod.fst).eraseDups)) hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.AggregateReplay.Goal_NonNullOnlySpillGroupPresence
