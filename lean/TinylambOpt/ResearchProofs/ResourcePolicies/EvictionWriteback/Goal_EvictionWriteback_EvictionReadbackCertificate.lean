import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_EvictionWriteback_EvictionReadbackCertificate
open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
theorem sound : EvictionReadbackCertificate.Goal := by
  intro s t wal hdirty hreclaim
  unfold finish at hreclaim
  split at hreclaim
  · -- guard true, returned s, but s.dirty = true contradicts reclaimable
    simp [reclaimable, hdirty] at hreclaim
  · -- guard false, updated
    simp only [reclaimable, Bool.and_eq_true, beq_iff_eq, Bool.not_eq_true'] at hreclaim
    rcases hreclaim with ⟨⟨hpins, hnotdirty⟩, hdisk⟩
    have htlive : s.live = t := by
      simpa using hnotdirty
    subst htlive
    refine ⟨rfl, ?_, by simpa using hpins⟩
    -- guard was false, so wal < t.wal is false
    rename_i hguard
    simp at hguard
    omega
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_EvictionWriteback_EvictionReadbackCertificate
