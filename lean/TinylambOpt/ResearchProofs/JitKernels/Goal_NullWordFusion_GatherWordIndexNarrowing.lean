import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_GatherWordIndexNarrowing

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

theorem refute : ¬ GatherWordIndexNarrowing.Goal := by
  intro h
  have hh := h ([0, 1] : List W) 63
  have contra : ¬
      (((List.range 32).map (fun j => (lanes ([0, 1] : List W) 95)[63 + j]?)) =
        ((List.range 32).map (fun j => some ((([0, 1] : List W)[63 / 64]?.getD 0).getLsbD ((63 + j) % 64))))) := by
    native_decide
  exact contra hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_NullWordFusion_GatherWordIndexNarrowing
