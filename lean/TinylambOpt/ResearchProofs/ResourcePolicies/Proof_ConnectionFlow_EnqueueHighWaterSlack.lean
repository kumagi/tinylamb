import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_EnqueueHighWaterSlack

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow


/-! High-water admission: if a connection's queue is below `high` and a single
new reply frame has at most `maxFrame` bytes, the post-admission byte count is
strictly below `high + maxFrame`. This bounds overshoot by one frame without
requiring socket service. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow.EnqueueHighWaterSlack.Goal := by
  intro fs high frame maxFrame hfs hframe
  simp only [List.flatten_append, List.flatten_singleton, List.length_append]
  omega
end TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_EnqueueHighWaterSlack
