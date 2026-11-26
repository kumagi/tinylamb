import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_BackpressureStableObservation
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- With distinct watermarks `lo < hi`, re-evaluating the blocked flag at the
same byte count is idempotent: a paused connection stays paused (lo < bytes
whenever hi ≤ bytes), and a running connection only blocks past `hi`, so no
pause/resume flip happens without byte progress. -/
theorem sound : BackpressureStableObservation.Goal := by
  intro lo hi paused bytes hlt
  cases paused
  · -- not paused: the flag reports `hi ≤ bytes`, which is already stable
    by_cases h : hi ≤ bytes
    · have h2 : lo < bytes := by omega
      simp [blocked, h, h2]
    · simp [blocked, h]
  · -- paused: the flag reports `lo < bytes`
    by_cases h : lo < bytes
    · simp [blocked, h]
    · have h2 : ¬ (hi ≤ bytes) := by omega
      simp [blocked, h, h2]

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_BackpressureStableObservation
