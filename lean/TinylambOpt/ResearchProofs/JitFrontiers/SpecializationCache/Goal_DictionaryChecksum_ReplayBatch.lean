import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Refuted by counterexample

- `DictionaryChecksum.ReplayBatch` — Reuse in a whole batch preserves the first failing
  ordinal as well as the value sequence.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryChecksum_ReplayBatch

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.DictionaryChecksum
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem refute : ¬ DictionaryChecksum.ReplayBatch.Goal := by
  intro h
  let L : Cfg := ⟨0, 0, 0, false, 0, [1,2]⟩
  let R : Cfg := {L with table := [2,1]}
  have hres := h [L] R [[some 0]]
  have hneq : [Out.val 1] ≠ [Out.val 2] := by native_decide
  apply hneq
  have hc1 : call DictionaryChecksum.shape [L] R [some 0] = Out.val 1 := by native_decide
  have hc2 : DictionaryChecksum.shape.reference R [some 0] = Out.val 2 := by native_decide
  simpa [hc1, hc2] using hres.1

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_DictionaryChecksum_ReplayBatch
