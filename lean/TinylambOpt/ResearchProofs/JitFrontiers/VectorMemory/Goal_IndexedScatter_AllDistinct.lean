import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `IndexedScatter.AllDistinct` — `∀ j n, n≤j.ids.length → (j.ids.take n).Nodup →
  ((List.range n).map (kernel.store j)).Nodup`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedScatter_AllDistinct

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.IndexedScatter
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : IndexedScatter.AllDistinct.Goal := by
  exact scatter_allDistinct

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedScatter_AllDistinct
