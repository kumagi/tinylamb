import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_LastFieldReadFootprint

open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

theorem refute : ¬ LastFieldReadFootprint.Goal := by
  intro h
  have hh := h 1 [0] 0 (by decide) (by decide) (by decide)
  have contra : ¬ within (pack 1 [0]) 1 0 8 := by
    unfold within
    native_decide
  exact contra hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_PackedFieldLoads_LastFieldReadFootprint
