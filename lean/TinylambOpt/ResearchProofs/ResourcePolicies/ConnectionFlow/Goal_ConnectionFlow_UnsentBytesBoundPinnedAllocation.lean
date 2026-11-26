import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_UnsentBytesBoundPinnedAllocation
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Refuted: logical bytes left to send do *not* cap the pinned allocation of a
partially consumed frame. A 10-byte frame with 5 bytes already sent has 5
unsent bytes (≤ cap 5) while the retained backing allocation is still 10. -/
theorem refute : ¬ UnsentBytesBoundPinnedAllocation.Goal := by
  intro h
  exact absurd (h (List.range 10) 5 5) (by decide)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_UnsentBytesBoundPinnedAllocation
