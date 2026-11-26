import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_OneVersion

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.NullPolicy
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : NullPolicy.OneVersion.Goal := by
  intro c r hacc
  unfold NullPolicy.shape
  dsimp
  cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
  | null =>
      have hnn : (Out.null == Out.null) = true := by rfl
      by_cases hm : c.mode = true <;> simp [h, hm, hnn]
  | fault =>
      have hfn : (Out.fault == Out.null) = false := by rfl
      by_cases hm : c.mode = true <;> simp [h, hm, hfn]
  | val n =>
      have hvn : (Out.val n == Out.null) = false := by rfl
      by_cases hm : c.mode = true <;> simp [h, hm, hvn]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_OneVersion
