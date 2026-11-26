import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `DictionaryChecksum.FreshEpoch` — `∀ a b, a.epoch≠b.epoch → compatible shape a
  b=false`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryChecksum_FreshEpoch

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.DictionaryChecksum
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : DictionaryChecksum.FreshEpoch.Goal := by
  intro a b h
  simp [DictionaryChecksum.shape, compatible]
  omega

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryChecksum_FreshEpoch
