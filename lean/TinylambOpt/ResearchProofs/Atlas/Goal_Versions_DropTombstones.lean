import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
Snapshot maintenance must preserve the rows visible to a SQL transaction across edits,
retention, and compaction.

Refuted by counterexample

- `Versions.DropTombstones` — `∀ es k t v, readAt (es.filter (fun e =>
  e.payload.isSome)) k t v = readAt es k t v`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Versions_DropTombstones

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.Versions

/-! Smallest counterexample: a tombstone published after the payload at the
same key/interval wins the read at `t = 2`; dropping it resurrects the old 7. -/
#eval let log : List Versions.Event :=
        [⟨3, 1, 0, 0, 10, some 7⟩, ⟨3, 2, 0, 0, 10, none⟩]
      (Versions.readAt log 3 2 1,
       Versions.readAt (log.filter (fun e => e.payload.isSome)) 3 2 1)

theorem disproof_Versions_DropTombstones : ¬ Versions.DropTombstones.Goal := by
  intro h
  let log : List Versions.Event :=
    [⟨3, 1, 0, 0, 10, some 7⟩, ⟨3, 2, 0, 0, 10, none⟩]
  have hh := h log 3 2 1
  have hneq : ¬ (Versions.readAt (log.filter (fun e => e.payload.isSome)) 3 2 1 =
      Versions.readAt log 3 2 1) := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.Atlas.Goal_Versions_DropTombstones
