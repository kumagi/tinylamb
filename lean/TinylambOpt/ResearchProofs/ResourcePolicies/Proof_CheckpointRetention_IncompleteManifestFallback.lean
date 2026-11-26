import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

namespace TinylambOpt.ResearchProofs.ResourcePolicies.CheckpointRetention.Goal_IncompleteManifestFallback

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention


/-! An incomplete (or not-yet-durable) generation at the head of the manifest
list is skipped by `find?`, so retrying selection yields the same fallback
generation as before that manifest was published. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

theorem sound :
    TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention.IncompleteManifestFallback.Goal := by
  intro gs disk wal g hg
  simp [choose, hg]
end TinylambOpt.ResearchProofs.ResourcePolicies.CheckpointRetention.Goal_IncompleteManifestFallback
