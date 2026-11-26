import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `BackwardDelta.DependenceGuard` — A lane may not overwrite a later lane’s input
  before grouped loads finish.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_BackwardDelta_DependenceGuard

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.BackwardDelta
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : BackwardDelta.DependenceGuard.Goal := by
  exact dependenceGuard_generic BackwardDelta.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_BackwardDelta_DependenceGuard
