import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `BackwardDelta.FirstZero` — `∀ j mem, j.src<mem.length → load kernel j mem 0=some 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_BackwardDelta_FirstZero

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.BackwardDelta
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : BackwardDelta.FirstZero.Goal := by
  exact delta_first

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_BackwardDelta_FirstZero
