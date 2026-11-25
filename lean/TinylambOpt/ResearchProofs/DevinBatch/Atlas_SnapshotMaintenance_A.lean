import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.Atlas_SnapshotMaintenance_A

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

/-!
All ten assigned goals already have verified covering theorems in prior proof
files. Each covering file was re-checked with
`lake env lean -DrelaxedAutoImplicit=false` and exits 0.

Versions (`...SnapshotMaintenance.Versions`), covered by
`TinylambOpt/ResearchProofs/Atlas_SnapshotMaintenance_Versions.lean`:
- `DominancePrune.Goal` — `sound_DominancePrune` (line 561)
- `RunTournament.Goal`  — `sound_RunTournament`  (line 444)
- `SlabReuse.Goal`      — `sound_SlabReuse`      (line 371)
- `LocalPublish.Goal`   — `sound_LocalPublish`   (line 420)

`Versions.DropTombstones.Goal` is documented false and refuted by
`disproof_DropTombstones` in `TinylambOpt/ResearchProofs/Atlas_StrongHypotheses.lean`
(line 26) and `disproof_Versions_DropTombstones` in
`TinylambOpt/ResearchProofs/Atlas/Goal_Versions_DropTombstones.lean` (line 18),
both using the concrete witness
`log := [⟨3,1,0,0,10,some 7⟩, ⟨3,2,0,0,10,none⟩]` checked by `native_decide`.

IntervalDeletes (`...SnapshotMaintenance.IntervalDeletes`), covered by
`TinylambOpt/ResearchProofs/Atlas_SnapshotMaintenance_IntervalDeletes.lean`:
- `SignedEndpoints.Goal` — `sound_SignedEndpoints` (line 44)
- `OverlapDebt.Goal`     — `sound_OverlapDebt`     (line 69)
- `RebaseMask.Goal`      — `sound_RebaseMask`      (line 104)
- `FooterRuns.Goal`      — `sound_FooterRuns`      (line 593)
- `LiveSelect.Goal`      — `sound_LiveSelect`      (line 633)

Per the BRIEFING.md prior-art rule these goals are reported ALREADY_PROVED
and intentionally omitted here.
-/

end TinylambOpt.ResearchProofs.DevinBatch.Atlas_SnapshotMaintenance_A
