import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Refuted by counterexample

- `DictionaryChecksum.DropLastField` — Speculative cache compaction: is removing the
  final key component observationally safe?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryChecksum_DropLastField

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.DictionaryChecksum
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem refute : ¬ DictionaryChecksum.DropLastField.Goal := by
  let a : Cfg := ⟨0, 0, 0, false, 0, [1,2]⟩
  let b : Cfg := {a with table := [1,3]}
  apply not_dropLastField_of DictionaryChecksum.shape a b [some 1]
  · rfl
  · rfl
  · native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryChecksum_DropLastField
