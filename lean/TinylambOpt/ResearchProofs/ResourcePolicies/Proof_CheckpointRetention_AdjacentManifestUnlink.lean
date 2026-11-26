import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

namespace TinylambOpt.ResearchProofs.ResourcePolicies.CheckpointRetention.Goal_AdjacentManifestUnlink

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention


/-! Checking only the immediate successor ignores older readers. A file that
is absent from the successor but still referenced by an older pinned manifest
must not be unlinked. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

theorem refute :
    ¬ TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention.AdjacentManifestUnlink.Goal := by
  intro h
  have hce := h [⟨0, 0, [0]⟩] ⟨0, 0, [0]⟩ ⟨1, 1, []⟩ 0 (by simp) (by simp)
  simp at hce
end TinylambOpt.ResearchProofs.ResourcePolicies.CheckpointRetention.Goal_AdjacentManifestUnlink
