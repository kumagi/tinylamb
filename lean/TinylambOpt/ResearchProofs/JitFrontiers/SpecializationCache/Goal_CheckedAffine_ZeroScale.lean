import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `CheckedAffine.ZeroScale` — `∀ c x, c.slot=0 → c.scale=0 → fits 64 c.literal →
  shape.code c [some x]=.val c.literal`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_CheckedAffine_ZeroScale

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.CheckedAffine
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem get0 (v : Int) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [some v] 0 = Out.val v := by rfl

private theorem get0none :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [none] 0 = Out.null := by rfl

theorem sound : CheckedAffine.ZeroScale.Goal := by
  intro c x hs hl hfit
  unfold CheckedAffine.shape
  dsimp
  rw [hs, hl]
  simp [mapV, get0]
  rw [show word 0 = 0 by rfl]
  simpa using word_of_fits c.literal hfit

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_CheckedAffine_ZeroScale
