import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryChecksum_OneVersion

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.DictionaryChecksum
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : DictionaryChecksum.OneVersion.Goal := by
  intro c r hacc
  unfold DictionaryChecksum.shape
  dsimp
  unfold dictionaryLoad dictionaryReference
  cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
  | null => simp [h]
  | fault => simp [h]
  | val x => simp [h]; simpa [List.getElem?_toArray] using load_eq_reference c.table x

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryChecksum_OneVersion
