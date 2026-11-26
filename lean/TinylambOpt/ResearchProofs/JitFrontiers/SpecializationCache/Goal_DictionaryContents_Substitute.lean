import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `DictionaryContents.Substitute` — A cached function compiled from another descriptor
  is safe on the new guard domain.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryContents_Substitute

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.DictionaryContents
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : DictionaryContents.Substitute.Goal := by
  apply substitute_of DictionaryContents.shape
  ·
    intro c r hacc
    unfold DictionaryContents.shape
    dsimp
    unfold dictionaryLoad dictionaryReference
    cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
    | null => simp [h]
    | fault => simp [h]
    | val x => simp [h]; simpa [List.getElem?_toArray] using load_eq_reference c.table x
  ·
    intro a b hk r
    have hs : (a.slot : Int) = b.slot := by
      simpa [DictionaryContents.shape] using congrArg List.head? hk
    have ht : a.table = b.table := by
      simpa [DictionaryContents.shape] using congrArg List.tail hk
    have hsl : a.slot = b.slot := by omega
    simp [DictionaryContents.shape, dictionaryLoad, hsl, ht]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryContents_Substitute
