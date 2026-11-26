import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `IndexedGather.UniqueReads` — `∀ j n, n≤j.ids.length → (addresses kernel j
  (List.range n)).length=(j.ids.take n).eraseDups.length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedGather_UniqueReads

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.IndexedGather
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : IndexedGather.UniqueReads.Goal := by
  exact uniqueReads

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedGather_UniqueReads
