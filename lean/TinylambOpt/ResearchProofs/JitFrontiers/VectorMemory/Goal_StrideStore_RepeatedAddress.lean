import TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common_VectorMemory

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.

Proved

- `StrideStore.RepeatedAddress` — `∀ j i, j.stride=0 → kernel.store j i=j.dst`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideStore_RepeatedAddress

open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
open TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory.StrideStore
open TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Common

theorem sound : StrideStore.RepeatedAddress.Goal := by
  exact strideStore_repeat

end TinylambOpt.ResearchProofs.JitFrontiers.VectorMemory.Goal_StrideStore_RepeatedAddress
