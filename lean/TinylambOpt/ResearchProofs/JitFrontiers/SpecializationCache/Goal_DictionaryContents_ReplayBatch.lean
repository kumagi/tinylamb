import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `DictionaryContents.ReplayBatch` — Reuse in a whole batch preserves the first failing
  ordinal as well as the value sequence.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryContents_ReplayBatch

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.DictionaryContents
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem hone : oneVersion DictionaryContents.shape := by
  intro c r hacc
  unfold DictionaryContents.shape
  dsimp
  unfold dictionaryLoad dictionaryReference
  cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
  | null => simp [h]
  | fault => simp [h]
  | val x => simp [h]; simpa [List.getElem?_toArray] using load_eq_reference c.table x
private theorem hsu : substitute DictionaryContents.shape := by
  apply substitute_of DictionaryContents.shape
  · exact hone
  ·
    intro a b hk r
    have hs : (a.slot : Int) = b.slot := by
      simpa [DictionaryContents.shape] using congrArg List.head? hk
    have ht : a.table = b.table := by
      simpa [DictionaryContents.shape] using congrArg List.tail hk
    have hsl : a.slot = b.slot := by omega
    simp [DictionaryContents.shape, dictionaryLoad, hsl, ht]
private theorem hcall : ∀ pool c r, call DictionaryContents.shape pool c r = DictionaryContents.shape.reference c r := by
  exact call_eq_reference DictionaryContents.shape hone hsu

theorem sound : DictionaryContents.ReplayBatch.Goal := by
  exact replayBatch_of DictionaryContents.shape hcall

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryContents_ReplayBatch
