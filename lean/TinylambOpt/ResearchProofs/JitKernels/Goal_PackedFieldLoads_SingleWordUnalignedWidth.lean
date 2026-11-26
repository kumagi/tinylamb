import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_SingleWordUnalignedWidth

open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

theorem refute : ¬ SingleWordUnalignedWidth.Goal := by
  intro h
  have hcanon : ∀ b ∈ List.range 20, b < 256 := by native_decide
  have hh := h (List.range 20) 63 1 (by omega) hcanon
  have contra : ¬ (load (List.range 20) 63 1 8 = takeAt (List.range 20) 63 1) := by
    native_decide
  exact contra hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_SingleWordUnalignedWidth
