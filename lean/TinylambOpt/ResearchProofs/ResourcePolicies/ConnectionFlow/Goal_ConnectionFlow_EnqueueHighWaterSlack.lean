import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_EnqueueHighWaterSlack
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- If the queued output is below the high watermark `high`, admitting one more
frame of size at most `maxFrame` overshoots the watermark by less than that
frame's size: the queue stays below `high + maxFrame`. -/
theorem sound : EnqueueHighWaterSlack.Goal := by
  intro fs high frame maxFrame h1 h2
  have hlen : (fs ++ [frame]).flatten.length = fs.flatten.length + frame.length := by
    simp [List.flatten_append]
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_EnqueueHighWaterSlack
