import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

namespace TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_UnsentBytesBoundPinnedAllocation

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow


/-! Refutation: counting only the unwritten suffix cannot bound memory when a
partially consumed frame keeps its whole backing allocation alive. A three-byte
frame with two bytes already acknowledged has one unwritten byte but still
occupies three bytes of storage. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow.UnsentBytesBoundPinnedAllocation.Goal := by
  intro h
  have hbad := h [1, 2, 3] 2 1 (by decide) (by decide)
  simp at hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.ConnectionFlow.Goal_UnsentBytesBoundPinnedAllocation
