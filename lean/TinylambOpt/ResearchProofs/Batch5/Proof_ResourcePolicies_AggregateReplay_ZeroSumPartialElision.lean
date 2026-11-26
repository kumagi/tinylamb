import TinylambOpt.ResearchGoals.QueryRewrites.Common
import TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.AggregateReplay.Goal_ZeroSumPartialElision

open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay


/-! Scratch proof: a zero-sum partial may still carry a non-zero COUNT, so
    dropping it changes the all-NULL / empty-group answer. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay

/-- A partial with state (0,1) contributes one non-NULL input of value zero.
    With an empty tail the answer changes from SOME 0 to NONE. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay.ZeroSumPartialElision.Goal := by
  intro h
  have hbad := h { id := 0, key := 0, partialState := (0, 1) } ([] : List Record) 0 (by decide)
  exact (by decide : ¬ answer (replay [{ id := 0, key := 0, partialState := (0, 1) }] 0) =
    answer (replay ([] : List Record) 0)) hbad
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.AggregateReplay.Goal_ZeroSumPartialElision
