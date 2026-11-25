import TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.Atlas_SnapshotMaintenance_B

open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance
open TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks

/-!
Nine of the ten assigned goals already have verified covering theorems in
prior proof files. Each covering file was re-checked with
`lake env lean -DrelaxedAutoImplicit=false` and exits 0.

ThresholdViews (`...SnapshotMaintenance.ThresholdViews`), covered by
`TinylambOpt/ResearchProofs/Atlas_SnapshotMaintenance_ThresholdViews.lean`:
- `ZeroCrossing.Goal`      — `sound_ZeroCrossing`      (line 296)
- `Coalesce.Goal`          — `sound_Coalesce`          (line 346)
- `SemijoinRepair.Goal`    — `sound_SemijoinRepair`    (line 373)
- `MarginCertificate.Goal` — `sound_MarginCertificate` (line 449)
- `ReplayLedger.Goal`      — `sound_ReplayLedger`      (line 488)

BatchWatermarks (`...SnapshotMaintenance.BatchWatermarks`), covered by
`TinylambOpt/ResearchProofs/Atlas_SnapshotMaintenance_BatchWatermarks.lean`:
- `PartialOpen.Goal`       — `sound_PartialOpen`       (line 91)
- `CoarseTime.Goal`        — `sound_CoarseTime`        (line 107)
- `HistoricalBand.Goal`    — `sound_HistoricalBand`    (line 156)
- `EquivalentHistory.Goal` — `sound_EquivalentHistory` (line 163)

`BatchWatermarks.FinalSummary.Goal` is a documented strong hypothesis that is
false (see `issues/research/atlas.md`, BatchWatermarks row). Two runs can
share the same stamp and the same total weight sum yet differ at an
intermediate snapshot. Concrete witness, matching the `#guard` check in the
goal module:

- `a = [(0,4),(1,-4)]`, `b = [(0,0),(1,0)]`.
- `stamp a = stamp b = 1` (both `fst` projections have max `1`).
- `(a.map Prod.snd).sum = 4 + (-4) = 0 = 0 + 0 = (b.map Prod.snd).sum`.
- `score 0 a = 4` (only `(0,4)` is visible at `t = 0`), while
  `score 0 b = 0` (only `(0,0)` is visible). Hence `score 0 a ≠ score 0 b`.

The full refutation is `FinalSummary_disproof` below; all arithmetic is
discharged by `decide` on the concrete lists.
-/

theorem FinalSummary_disproof :
    ¬ TinylambOpt.ResearchGoals.Atlas.SnapshotMaintenance.BatchWatermarks.FinalSummary.Goal := by
  intro h
  have hscore := h ([(0,4),(1,-4)] : List Row) ([(0,0),(1,0)] : List Row)
    (by decide) (by decide) 0
  have h1 : score 0 ([(0,4),(1,-4)] : List Row) = 4 := by decide
  have h2 : score 0 ([(0,0),(1,0)] : List Row) = 0 := by decide
  rw [h1, h2] at hscore
  omega

end TinylambOpt.ResearchProofs.DevinBatch.Atlas_SnapshotMaintenance_B
