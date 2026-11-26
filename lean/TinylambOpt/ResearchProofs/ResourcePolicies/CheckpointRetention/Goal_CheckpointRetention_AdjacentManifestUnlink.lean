import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_AdjacentManifestUnlink
open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
theorem refute : ¬ AdjacentManifestUnlink.Goal := by
  intro h
  let older : List Generation := [⟨1, 1, [42]⟩]
  let current : Generation := ⟨2, 2, [42]⟩
  let successor : Generation := ⟨3, 3, []⟩
  have h' := h older current successor 42 (by simp [current]) (by simp [successor])
  simp [older, successor] at h'
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_CheckpointRetention_AdjacentManifestUnlink
