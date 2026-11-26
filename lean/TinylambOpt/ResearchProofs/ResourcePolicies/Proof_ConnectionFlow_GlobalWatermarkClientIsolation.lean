import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_GlobalWatermarkClientIsolation

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow


/-! Refutation: a single shared byte watermark does not isolate clients. Bytes
queued for other connections can push the shared count across the high
watermark and pause a client that is itself below it. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow.GlobalWatermarkClientIsolation.Goal := by
  intro h
  have hbad := h [4] 1 0 5 (by decide)
  simp [blocked] at hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_GlobalWatermarkClientIsolation
