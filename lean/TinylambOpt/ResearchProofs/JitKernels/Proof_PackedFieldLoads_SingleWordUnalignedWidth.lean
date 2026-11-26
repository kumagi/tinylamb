import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
A single eight-byte speculative load is not enough at every packing phase:
a 63-bit lane starting in the high part of a byte needs a ninth byte. The
counterexample uses canonical wire bytes, so the failure is not an artifact of
noncanonical input.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PackedFieldLoads_SingleWordUnalignedWidth
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

theorem refute : ¬ SingleWordUnalignedWidth.Goal := by
  intro h
  have hcanon : ∀ b ∈ List.range 20, b < 256 := by native_decide
  have heq := h (List.range 20) 63 1 (by decide) hcanon
  exact (by native_decide :
    load (List.range 20) 63 1 8 ≠ takeAt (List.range 20) 63 1) heq

end Nov26.Proof_PackedFieldLoads_SingleWordUnalignedWidth
