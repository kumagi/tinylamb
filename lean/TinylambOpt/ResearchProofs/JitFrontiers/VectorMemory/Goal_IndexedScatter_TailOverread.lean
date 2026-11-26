import Std
import TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedScatter_TailOverread

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

theorem refute : ¬ IndexedScatter.TailOverread.Goal := by
  intro h
  let j : Job := ⟨1,0,1,0,[],[]⟩
  have hv : valid IndexedScatter.kernel j 0 (selected j [0]) := by simp [j, selected, valid]
  have hc := h j 0 [0] hv 1 (by simp [j, addresses, IndexedScatter.kernel])
  omega

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedScatter_TailOverread
