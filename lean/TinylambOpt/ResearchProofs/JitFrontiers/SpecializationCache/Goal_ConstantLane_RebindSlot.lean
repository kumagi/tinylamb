import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `ConstantLane.RebindSlot` — `∀ c j, shape.key {c with slot:=j}=shape.key c`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ConstantLane_RebindSlot

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.ConstantLane
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : ConstantLane.RebindSlot.Goal := by
  intro c j
  rfl

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ConstantLane_RebindSlot
