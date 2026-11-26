import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_CheckedAffine_DropLastField

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.CheckedAffine
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem refute : ¬ CheckedAffine.DropLastField.Goal := by
  let a : Cfg := ⟨0, 0, 0, false, 0, []⟩
  let b : Cfg := {a with literal := 1}
  apply not_dropLastField_of CheckedAffine.shape a b [some 1]
  · rfl
  · rfl
  · native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_CheckedAffine_DropLastField
