import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `PowerRemainder.NegativeScaleAlias` — `∀ c, c.scale<0 → ∀ r, shape.code c
  r=shape.code {c with scale:=0} r`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_PowerRemainder_NegativeScaleAlias

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.PowerRemainder
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : PowerRemainder.NegativeScaleAlias.Goal := by
  intro c h r
  simp [PowerRemainder.shape, Int.toNat_of_nonpos (by omega : c.scale ≤ 0)]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_PowerRemainder_NegativeScaleAlias
