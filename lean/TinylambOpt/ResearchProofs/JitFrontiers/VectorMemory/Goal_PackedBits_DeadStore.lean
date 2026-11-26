import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `PackedBits.DeadStore` — A vector scatter is last-writer-wins at duplicate addresses.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_PackedBits_DeadStore

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.PackedBits
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : PackedBits.DeadStore.Goal := by
  exact deadStore_generic PackedBits.kernel

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_PackedBits_DeadStore
