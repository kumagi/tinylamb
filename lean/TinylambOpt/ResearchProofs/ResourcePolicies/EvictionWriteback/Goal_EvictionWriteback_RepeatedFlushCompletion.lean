import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_EvictionWriteback_RepeatedFlushCompletion
open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
theorem sound : RepeatedFlushCompletion.Goal := by
  intro s t wal
  simp only [finish]
  split
  · rename_i h
    simp [finish, h]
  · rename_i h
    have hne : (t.incarnation != t.incarnation) = false := by simp
    have hwal : ¬ (wal < t.wal) := by
      simp at h
      omega
    have hrev : (some t).any (fun d => d.incarnation == t.incarnation && t.revision < d.revision) = false := by
      simp
    simp [finish, hne, hrev, hwal]
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_EvictionWriteback_RepeatedFlushCompletion
