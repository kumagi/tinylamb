import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_GlobalWatermarkClientIsolation
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Refuted: a single shared watermark does *not* isolate clients. With
`lo = 0`, `hi = 2`, a client holding 1 queued byte is unblocked on its own, but
one other client queueing 1 more byte pushes the shared counter past `hi`, so
`blocked 0 2 false 1 ≠ blocked 0 2 false (1 + [1].sum)`. -/
theorem refute : ¬ GlobalWatermarkClientIsolation.Goal := by
  intro h
  exact absurd (h [1] 1 0 2 (by omega)) (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_GlobalWatermarkClientIsolation
