import TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_NewestManifestOnlyRecovery

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention


/-! Recovery must continue scanning past an incomplete newest manifest. The
goal would make selection depend only on the first entry, which loses the
older restorable generation; the module's own `#guard` already exhibits it. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

theorem refute :
    ¬ TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention.NewestManifestOnlyRecovery.Goal := by
  intro h
  have hce := h [⟨2, 12, [5]⟩, ⟨1, 8, [4]⟩] 12 [4]
  simp [choose, ready] at hce
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CheckpointRetention.Goal_NewestManifestOnlyRecovery
