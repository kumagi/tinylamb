import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
**Refuted: one 8-byte load does not cover every ≤ 63-bit field.** With
`bs = List.range 20`, `w = 63`, `i = 1` the field starts at bit 63, i.e. byte 7
with intra-byte phase 7, and spans 70 bits — nine bytes, not eight. The masked
8-byte load truncates the field to its low 57 bits (`0x5f...` vs the reference
`takeAt` value), so the claimed equality `load bs w i 8 = takeAt bs w i` fails
and `SingleWordUnalignedWidth.Goal` is false. Decoders must keep the two-word
path for any field whose phase plus width exceeds 64 bits.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_SingleWordUnalignedWidth
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

theorem refute : ¬ SingleWordUnalignedWidth.Goal := by
  intro h
  have hcanon : ∀ b ∈ List.range 20, b < 256 := by decide
  have heq := h (List.range 20) 63 1 (by decide) hcanon
  exact (by decide : load (List.range 20) 63 1 8 ≠ takeAt (List.range 20) 63 1) heq

end TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_SingleWordUnalignedWidth
