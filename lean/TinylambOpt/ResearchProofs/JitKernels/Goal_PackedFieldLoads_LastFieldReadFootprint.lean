import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_LastFieldReadFootprint

open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

/-! A one-bit column packs into a single byte, so an eight-byte speculative
read is not inside its allocation. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads.LastFieldReadFootprint.Goal := by
  rw [LastFieldReadFootprint.Goal]
  intro h
  have hh := h 1 [0] 0 (by decide) (by decide) (by decide)
  have hbad : ¬ within (pack 1 [0]) 1 0 8 := by
    unfold within
    native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_LastFieldReadFootprint
