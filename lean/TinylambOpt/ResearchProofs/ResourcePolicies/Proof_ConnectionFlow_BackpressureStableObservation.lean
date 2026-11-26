import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_BackpressureStableObservation

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow


/-! Hysteresis stability: with a low watermark strictly below the high one,
re-evaluating the pause condition with the newly returned pause flag cannot
flip the observation without any byte progress. Both the paused and unpaused
fixed points are preserved. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow.BackpressureStableObservation.Goal := by
  intro lo hi paused bytes hlt
  cases paused
  · by_cases hhi : hi ≤ bytes
    · have hlo : lo < bytes := by omega
      simp [blocked, hhi, hlo]
    · simp [blocked, hhi]
  · by_cases hlo : lo < bytes
    · simp [blocked, hlo]
    · have hhi : ¬ hi ≤ bytes := by omega
      simp [blocked, hlo, hhi]
end TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_BackpressureStableObservation
