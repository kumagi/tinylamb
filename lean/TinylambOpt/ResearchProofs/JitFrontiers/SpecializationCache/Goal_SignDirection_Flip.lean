import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_SignDirection_Flip

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.SignDirection
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem get0 (v : Int) :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [some v] 0 = Out.val v := by rfl

private theorem get0none :
    TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get [none] 0 = Out.null := by rfl

theorem sound : SignDirection.Flip.Goal := by
  intro c x hs hx
  unfold SignDirection.shape
  dsimp
  by_cases hlt : x < c.literal
  · have hgt : ¬ c.literal < x := by omega
    by_cases hm : c.mode <;> simp [mapV, hs, hm, hlt, hgt, get0]
  · have hgt : c.literal < x := by omega
    by_cases hm : c.mode <;> simp [mapV, hs, hm, hlt, hgt, get0]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_SignDirection_Flip
