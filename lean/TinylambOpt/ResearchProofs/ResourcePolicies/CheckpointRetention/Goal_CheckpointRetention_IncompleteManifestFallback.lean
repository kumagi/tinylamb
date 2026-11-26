import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_IncompleteManifestFallback
open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
theorem sound : IncompleteManifestFallback.Goal := by
  intro gs disk wal g h
  simp [choose, h]
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_IncompleteManifestFallback
