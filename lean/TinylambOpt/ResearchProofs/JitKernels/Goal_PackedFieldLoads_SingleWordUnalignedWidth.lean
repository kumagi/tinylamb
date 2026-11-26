import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

-- Refutation: a 63-bit field at bit phase 7 needs the high byte of a ninth byte window.

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_SingleWordUnalignedWidth

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads.SingleWordUnalignedWidth.Goal := by
  intro h
  have c := h (List.replicate 15 0 ++ [1] : List Nat) 63 1 (by native_decide) (by native_decide)
  exact (by native_decide : ¬ (
    TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads.load
        (List.replicate 15 0 ++ [1] : List Nat) 63 1 8 =
      TinylambOpt.ResearchGoals.Compression.Wire.takeAt
        (List.replicate 15 0 ++ [1] : List Nat) 63 1)) c

end TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_SingleWordUnalignedWidth
