import Std
import TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Refuted by counterexample

- `ForwardCopy.TailOverread` — Aggressive tail widening: legality of selected lanes
  suffices to read all physical lanes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ForwardCopy_TailOverread

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

theorem refute : ¬ ForwardCopy.TailOverread.Goal := by
  intro h
  let j : Job := ⟨1,0,1,0,[],[]⟩
  have hv : valid ForwardCopy.kernel j 0 (selected j [0]) := by simp [j, selected, valid]
  have hc := h j 0 [0] hv 1 (by simp [j, addresses, ForwardCopy.kernel])
  omega

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_ForwardCopy_TailOverread
