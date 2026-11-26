import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `IndexedGather.SameCodes` — `∀ j mem i t, j.ids[i]?=j.ids[t]? → load kernel j mem
  i=load kernel j mem t`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedGather_SameCodes

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.IndexedGather
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : IndexedGather.SameCodes.Goal := by
  exact gather_same

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_IndexedGather_SameCodes
