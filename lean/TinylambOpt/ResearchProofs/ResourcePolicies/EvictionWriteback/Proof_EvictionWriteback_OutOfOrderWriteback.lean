import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_EvictionWriteback_OutOfOrderWriteback
open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
theorem sound : OutOfOrderWriteback.Goal := by
  intro s old new wal hinc hlive hrev hwal hdisk
  have hguard_new : (new.incarnation != s.live.incarnation || wal < new.wal ||
      s.disk.any (fun d => d.incarnation == new.incarnation && new.revision < d.revision)) = false := by
    have hdisk' : s.disk.any (fun d => d.incarnation == new.incarnation && new.revision < d.revision) = false := by
      cases h : s.disk <;> simp_all
      omega
    simp [hlive, hwal, hdisk']
  have hfinish_new : finish s new wal = {s with disk := some new, dirty := s.live != new} := by
    simp [finish, hguard_new]
  rw [hfinish_new]
  unfold finish
  simp only
  have hguard_old : (old.incarnation != s.live.incarnation || wal < old.wal ||
      (some new).any (fun d => d.incarnation == old.incarnation && old.revision < d.revision)) = true := by
    simp [hinc, hlive, hrev]
  split
  · rfl
  · rename_i hg
    simp_all
end Nov26.Proof_EvictionWriteback_OutOfOrderWriteback
