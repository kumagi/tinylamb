# BitLayoutsA results

Scope: the 50 fixed Goals for `RowMajor`, `Vertical`, `SerpentineRow`,
`SerpentinePlane`, and `TileTranspose`.

Proof aggregator:
`lean/TinylambOpt/ResearchProofs/StorageExecution/BitLayoutsA.lean`
(imports exactly 50 per-Goal modules).

Verification (from `lean/`):

```bash
~/.elan/bin/lake build \
  TinylambOpt.ResearchProofs.StorageExecution.BitLayoutsA \
  TinylambOpt.ResearchProofs.StorageExecution.BitLayoutsB
```

Result: **50 PROVEN, 0 REFUTED**. All 50 Goals have a public theorem of the
exact Goal type. The combined BitLayouts axiom audit reports 100 proven, 0
refuted, with no `sorryAx` or unexpected axioms. No false Goal was found.
The previously incomplete `RowMajor.MaskedPatch` and
`RowMajor.PlanePredicate` modules were repaired, and the missing
`RowMajor.PointSpan`, `TileTranspose.TileCharge`, and
`TileTranspose.Transcode` proofs were added.

## RowMajor

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor.Lossless.Goal` | PROVEN | Consecutive bit placement reconstructs each value modulo `2^w`. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor.Nonoverlap.Goal` | PROVEN | Equality of positions forces equal row and lane, including zero width. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor.SharedEdgeByte.Goal` | PROVEN | Adjacent rows share a byte exactly at a non-byte-aligned boundary. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor.MaskedPatch.Goal` | PROVEN | Repaired via bitwise image characterization and injective row cells. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor.PlanePredicate.Goal` | PROVEN | Repaired by matching serialized plane bits to source lane bits. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor.SparseGather.Goal` | PROVEN | Every fetched byte belongs to the selected rows' address set. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor.Transcode.Goal` | PROVEN | Lossless reads pack to the same tight row-major word. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor.DictionaryProbe.Goal` | PROVEN | Dictionary lookup commutes with lossless low-width reads. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor.PageBudget.Goal` | PROVEN | Unique addressed bytes are bounded by capacity and per-row lanes. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor.PointSpan.Goal` | PROVEN | A contiguous span meets at most `byteCount (w + 7)` bytes. |

## Vertical

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical.Lossless.Goal` | PROVEN | Plane-major positions preserve each significance bit. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical.Nonoverlap.Goal` | PROVEN | Plane and ordinal coordinates are recoverable from a bit position. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical.PlaneScanSpan.Goal` | PROVEN | An aligned `n`-row plane scans exactly `n / 8` bytes. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical.MaskedPatch.Goal` | PROVEN | Row patching changes only that ordinal's bit in every plane. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical.PlanePredicate.Goal` | PROVEN | Plane scans return exactly the rows whose source bit is set. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical.SparseGather.Goal` | PROVEN | Gather results depend only on selected-row footprint bytes. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical.Transcode.Goal` | PROVEN | Plane reads convert to the canonical packed representation. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical.DictionaryProbe.Goal` | PROVEN | Lossless plane reads preserve dictionary indices. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical.PageBudget.Goal` | PROVEN | Addressed bytes obey both frame and naive per-lane bounds. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.Vertical.AlignedPlane.Goal` | PROVEN | Byte-aligned row counts make every plane start on a byte boundary. |

## SerpentineRow

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow.Lossless.Goal` | PROVEN | Alternating within-row reversal still reconstructs low-width values. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow.Nonoverlap.Goal` | PROVEN | Row blocks are disjoint and reversal is injective within each block. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow.DoubleStride.Goal` | PROVEN | Advancing two rows preserves parity and adds exactly `2*w`. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow.MaskedPatch.Goal` | PROVEN | Masked replacement respects the alternating lane permutation. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow.PlanePredicate.Goal` | PROVEN | Plane membership agrees with source significance after reversal. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow.SparseGather.Goal` | PROVEN | Selected ordinals determine all bytes needed by a gather. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow.Transcode.Goal` | PROVEN | Lane reads undo the serpentine permutation before packing. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow.DictionaryProbe.Goal` | PROVEN | Dictionary probes see the same low-width values. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow.PageBudget.Goal` | PROVEN | Reversal does not increase the addressed-byte bounds. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentineRow.FacingEndpoints.Goal` | PROVEN | Facing endpoints of consecutive opposite-direction rows are adjacent. |

## SerpentinePlane

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane.Lossless.Goal` | PROVEN | Odd-plane ordinal reversal preserves all source bits. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane.Nonoverlap.Goal` | PROVEN | Plane blocks are disjoint and each reversal is injective. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane.SharedPlaneEdge.Goal` | PROVEN | Neighboring opposite-direction planes share a byte exactly across a partial boundary. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane.MaskedPatch.Goal` | PROVEN | Row replacement changes only the corresponding cells in each plane. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane.PlanePredicate.Goal` | PROVEN | Physical plane bits identify exactly the matching source rows. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane.SparseGather.Goal` | PROVEN | Gathers are insensitive outside the selected rows' byte set. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane.Transcode.Goal` | PROVEN | Plane reads restore ordinal order and canonical packing. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane.DictionaryProbe.Goal` | PROVEN | Reversal leaves dictionary probe values unchanged. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane.PageBudget.Goal` | PROVEN | Plane folding respects capacity and per-row read bounds. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane.FoldedBoundary.Goal` | PROVEN | The last ordinal of facing planes occupies consecutive positions. |

## TileTranspose

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose.Lossless.Goal` | PROVEN | Eight-row transposed tiles decode every source lane exactly. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose.Nonoverlap.Goal` | PROVEN | Tile, lane, and within-tile ordinal are uniquely recoverable. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose.OneBytePerPlane.Goal` | PROVEN | A full tile addresses exactly one byte for each of its `w` lanes. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose.MaskedPatch.Goal` | PROVEN | Tile transposition commutes with clearing and replacing one row. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose.PlanePredicate.Goal` | PROVEN | A transposed significance bit identifies exactly its source rows. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose.SparseGather.Goal` | PROVEN | Selected tile rows expose all bytes required by their lane reads. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose.Transcode.Goal` | PROVEN | Added by reducing lossless tile reads to canonical packed words. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose.DictionaryProbe.Goal` | PROVEN | Transposed reads preserve dictionary lookup indices. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose.PageBudget.Goal` | PROVEN | Padded tile capacity and per-row footprint bounds both hold. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.TileTranspose.TileCharge.Goal` | PROVEN | Added by dividing the tile-local position by eight. |
