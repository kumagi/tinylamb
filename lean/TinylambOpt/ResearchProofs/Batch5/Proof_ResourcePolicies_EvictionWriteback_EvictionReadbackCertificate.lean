import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_EvictionReadbackCertificate

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback


/-!
A reclamable slot after a flush completion certifies that the ticket image is
exactly the live image, the durable LSN covers its WAL dependency, and no pin
remains. A blocked completion leaves the dirty bit set and cannot qualify.
This is the read-back certificate needed before reusing a page slot.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

private def flushBlocked (s : Slot) (t : Image) (wal : Nat) : Bool :=
  t.incarnation != s.live.incarnation || wal < t.wal ||
    s.disk.any (fun d => d.incarnation == t.incarnation && t.revision < d.revision)

private theorem finish_blocked (s : Slot) (t : Image) (wal : Nat) (h : flushBlocked s t wal = true) :
    finish s t wal = s := by
  unfold finish flushBlocked at *
  simp [h]

private theorem finish_allowed (s : Slot) (t : Image) (wal : Nat) (h : flushBlocked s t wal = false) :
    finish s t wal = {s with disk := some t, dirty := s.live != t} := by
  unfold finish flushBlocked at *
  simp [h]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback.EvictionReadbackCertificate.Goal := by
  intro s t wal hdirty hrec
  by_cases hg : flushBlocked s t wal = true
  · rw [finish_blocked s t wal hg] at hrec
    unfold reclaimable at hrec
    simp [hdirty] at hrec
  · have hf := finish_allowed s t wal (by simp [hg])
    rw [hf] at hrec
    unfold reclaimable at hrec
    simp at hrec
    have hl : t = s.live := hrec.2
    have hw : t.wal ≤ wal := by
      unfold flushBlocked at hg
      simp at hg
      omega
    exact ⟨hl, hw, hrec.1.1⟩
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_EvictionReadbackCertificate
