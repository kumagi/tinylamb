# BitLayoutsB results

Scope: the 50 fixed Goals for `ByteAlignedRow`, `ByteAlignedPlane`,
`NetworkBitOrder`, `RotatedPlane`, and `SplitNibble`.

Proof aggregator:
`lean/TinylambOpt/ResearchProofs/StorageExecution/BitLayoutsB.lean`
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
The missing `RotatedPlane.BankCycle` proof was added; the other 49 modules
compile unchanged.

## ByteAlignedRow

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow.Lossless.Goal` | PROVEN | Byte-aligned row blocks preserve low-width lane values. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow.Nonoverlap.Goal` | PROVEN | Aligned block length separates rows and lanes injectively. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow.DisjointRowWrites.Goal` | PROVEN | Distinct rows never address the same byte. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow.MaskedPatch.Goal` | PROVEN | Row masks affect only the aligned block being replaced. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow.PlanePredicate.Goal` | PROVEN | Aligned physical bits match source plane membership. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow.SparseGather.Goal` | PROVEN | Gathers use only bytes in selected rows' footprints. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow.Transcode.Goal` | PROVEN | Aligned reads produce the canonical packed word. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow.DictionaryProbe.Goal` | PROVEN | Dictionary indices agree with lossless aligned reads. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow.PageBudget.Goal` | PROVEN | Alignment padding is included in both byte bounds. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedRow.PointBytes.Goal` | PROVEN | A row footprint contains exactly `byteCount w` bytes. |

## ByteAlignedPlane

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane.Lossless.Goal` | PROVEN | Independently aligned planes preserve every source bit. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane.Nonoverlap.Goal` | PROVEN | Aligned plane blocks and ordinals form an injective grid. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane.PlaneDirectory.Goal` | PROVEN | Capacity is exactly `w * byteCount n` bytes. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane.MaskedPatch.Goal` | PROVEN | Replacing a row changes only its bit in each plane. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane.PlanePredicate.Goal` | PROVEN | Each physical plane returns exactly the source bit's rows. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane.SparseGather.Goal` | PROVEN | Gather agreement follows from the selected footprint. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane.Transcode.Goal` | PROVEN | Plane reads convert to canonical row-major packing. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane.DictionaryProbe.Goal` | PROVEN | Plane alignment does not alter dictionary values. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane.PageBudget.Goal` | PROVEN | Unique plane bytes satisfy frame and per-row bounds. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.ByteAlignedPlane.AppendWithinPad.Goal` | PROVEN | Appending within the final padding byte leaves positions unchanged. |

## NetworkBitOrder

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder.Lossless.Goal` | PROVEN | Per-byte bit reversal preserves all low-width source bits. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder.Nonoverlap.Goal` | PROVEN | Byte identity and within-byte reversal are jointly injective. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder.BitReversal.Goal` | PROVEN | Reversed bit offsets sum to seven and retain the same byte. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder.MaskedPatch.Goal` | PROVEN | Row masks and replacement bits follow the reversed byte order. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder.PlanePredicate.Goal` | PROVEN | Reversed physical bits still identify exact source rows. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder.SparseGather.Goal` | PROVEN | Reversal does not change which source bytes are addressed. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder.Transcode.Goal` | PROVEN | Reversed reads pack into canonical little-endian words. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder.DictionaryProbe.Goal` | PROVEN | Dictionary probes observe the original low-width values. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder.PageBudget.Goal` | PROVEN | Reversal preserves capacity and per-lane byte bounds. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.NetworkBitOrder.ByteFootprint.Goal` | PROVEN | Reversing bits within bytes leaves row byte footprints unchanged. |

## RotatedPlane

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane.Lossless.Goal` | PROVEN | Rotating ordinals within each plane preserves all source bits. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane.Nonoverlap.Goal` | PROVEN | Disjoint planes and modular rotation give injective positions. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane.RotatedLocations.Goal` | PROVEN | For positive `n`, each rotated plane is a permutation of its addresses. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane.MaskedPatch.Goal` | PROVEN | Row patching changes only the rotated cell in each plane. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane.PlanePredicate.Goal` | PROVEN | Rotated plane bits recover exact source row membership. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane.SparseGather.Goal` | PROVEN | Gathers depend only on selected rows' rotated footprints. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane.Transcode.Goal` | PROVEN | Rotated reads restore canonical packed values. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane.DictionaryProbe.Goal` | PROVEN | Rotation preserves dictionary probe indices. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane.PageBudget.Goal` | PROVEN | Rotation respects both physical and naive byte budgets. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RotatedPlane.BankCycle.Goal` | PROVEN | Added using `(i + n + b) % n = (i + b) % n` for `0 < n`. |

## SplitNibble

| Full Goal declaration | Status | Note |
|---|---|---|
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble.Lossless.Goal` | PROVEN | Local low nibbles and transposed high planes decode exactly. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble.Nonoverlap.Goal` | PROVEN | The low-nibble and high-plane regions are disjoint and injective. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble.MixedPointCost.Goal` | PROVEN | A mixed row touches at most one low-nibble region plus high-plane bytes. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble.MaskedPatch.Goal` | PROVEN | Clearing and replacing a row respects both storage regions. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble.PlanePredicate.Goal` | PROVEN | Both local and transposed bits identify exact source rows. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble.SparseGather.Goal` | PROVEN | Gathers use exactly the selected rows' combined footprints. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble.Transcode.Goal` | PROVEN | Split reads reconstruct canonical packed words. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble.DictionaryProbe.Goal` | PROVEN | Dictionary probes see unchanged low-width values. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble.PageBudget.Goal` | PROVEN | The split representation obeys both byte-count bounds. |
| `TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SplitNibble.LowNibbleLocal.Goal` | PROVEN | Four local low lanes place two consecutive values in one byte. |
