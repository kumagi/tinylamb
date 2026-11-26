import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_CheckpointRetention_NewestManifestOnlyRecovery
open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
theorem refute : ¬ NewestManifestOnlyRecovery.Goal := by
  intro h
  let gs : List Generation := [⟨2, 12, [5]⟩, ⟨1, 8, [4]⟩]
  have h' := h gs 12 [4]
  simp [choose, ready, gs] at h'
end Nov26.Proof_CheckpointRetention_NewestManifestOnlyRecovery
