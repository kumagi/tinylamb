import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_OutOfOrderWriteback

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback


/-!
Out-of-order flush completions cannot roll back a newer durable image. Once a
newer same-incarnation ticket is written, a late older ticket is rejected by
the per-incarnation disk-revision fence. This keeps the backing image at the
maximum completed revision regardless of completion order.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

private def flushBlocked (s : Slot) (t : Image) (wal : Nat) : Bool :=
  t.incarnation != s.live.incarnation || wal < t.wal ||
    s.disk.any (fun d => d.incarnation == t.incarnation && t.revision < d.revision)

private theorem finish_allowed (s : Slot) (t : Image) (wal : Nat) (h : flushBlocked s t wal = false) :
    finish s t wal = {s with disk := some t, dirty := s.live != t} := by
  unfold finish flushBlocked at *
  simp [h]

private theorem no_newer_disk (s : Slot) (new : Image)
    (h : s.disk.all (fun d => d.incarnation != new.incarnation || d.revision ≤ new.revision) = true) :
    s.disk.any (fun d => d.incarnation == new.incarnation && new.revision < d.revision) = false := by
  cases hsd : s.disk with
  | none => simp [hsd]
  | some d =>
      simp [hsd] at h ⊢
      intro hd
      exact h.elim (fun hne => False.elim (hne hd)) id

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback.OutOfOrderWriteback.Goal := by
  intro s old new wal hinc hlive hrev hwal hdisk
  have hany : s.disk.any (fun d => d.incarnation == new.incarnation && new.revision < d.revision) = false :=
    no_newer_disk s new hdisk
  have hg1 : flushBlocked s new wal = false := by
    unfold flushBlocked
    simp [hlive, hany]
    omega
  have hs1 : finish s new wal = {s with disk := some new, dirty := s.live != new} :=
    finish_allowed s new wal hg1
  rw [hs1]
  unfold finish
  have hany2 : (some new).any (fun d => d.incarnation == old.incarnation && old.revision < d.revision) = true := by
    simp [hinc, hrev]
  simp [hany2]
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.EvictionWriteback.Goal_OutOfOrderWriteback
