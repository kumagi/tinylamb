import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryChecksum_EvictWhileHeld

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.DictionaryChecksum
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem refute : ¬ DictionaryChecksum.EvictWhileHeld.Goal := by
  intro h
  let L : Cfg := ⟨0, 0, 0, false, 0, [1,2]⟩
  let R : Cfg := {L with table := [2,1]}
  have hlookup : lookup DictionaryChecksum.shape [L] R = some L := by
    simp [lookup, compatible, DictionaryChecksum.shape, L, R, signature]
  have heq := h [L] R L [some 0] hlookup (by rfl)
  have hneq : DictionaryChecksum.shape.code L [some 0] ≠ call DictionaryChecksum.shape [] R [some 0] := by native_decide
  exact hneq heq

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryChecksum_EvictWhileHeld
