import Std
import TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_RingWindow_TailOverread

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

theorem refute : ¬ RingWindow.TailOverread.Goal := by
  intro h
  let j : Job := ⟨1,0,1,0,[],[]⟩
  have hv : valid RingWindow.kernel j 0 (selected j [0]) := by simp [j, selected, valid]
  have hc := h j 0 [0] hv 1 (by simp [j, addresses, RingWindow.kernel])
  omega

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_RingWindow_TailOverread
