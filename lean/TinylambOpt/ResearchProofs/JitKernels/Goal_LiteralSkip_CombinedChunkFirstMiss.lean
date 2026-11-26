import TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_CombinedChunkFirstMiss

open TinylambOpt.ResearchGoals.JitKernels.LiteralSkip

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.LiteralSkip.CombinedChunkFirstMiss.Goal := by
  intro h
  let p : B := [97, 98]
  let a : B := [97]
  let b : B := [98]
  have hpa : starts p a = [] := by native_decide
  have hpb : starts p b = [] := by native_decide
  have hres := h p a b hpa hpb
  have hbad : starts p (a ++ b) ≠ [] := by native_decide
  exact hbad hres

end TinylambOpt.ResearchProofs.JitKernels.Goal_LiteralSkip_CombinedChunkFirstMiss
