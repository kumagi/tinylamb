import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryContents_DropLastField

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.DictionaryContents
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem refute : ¬ DictionaryContents.DropLastField.Goal := by
  let a : Cfg := ⟨0, 0, 0, false, 0, [1,2]⟩
  let b : Cfg := {a with table := [1,3]}
  apply not_dropLastField_of DictionaryContents.shape a b [some 1]
  · rfl
  · rfl
  · native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryContents_DropLastField
