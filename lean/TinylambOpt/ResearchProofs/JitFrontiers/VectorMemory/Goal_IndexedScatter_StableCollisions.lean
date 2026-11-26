import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `IndexedScatter.StableCollisions` — `∀ j i t, j.ids[i]?=j.ids[t]? → kernel.store j
  i=kernel.store j t`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedScatter_StableCollisions

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.IndexedScatter
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : IndexedScatter.StableCollisions.Goal := by
  exact scatter_stable

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedScatter_StableCollisions
