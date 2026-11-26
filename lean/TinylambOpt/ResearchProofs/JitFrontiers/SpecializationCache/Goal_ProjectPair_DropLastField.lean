import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Refuted by counterexample

- `ProjectPair.DropLastField` — Speculative cache compaction: is removing the final key
  component observationally safe?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ProjectPair_DropLastField

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.ProjectPair
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem refute : ¬ ProjectPair.DropLastField.Goal := by
  let a : Cfg := ⟨0, 0, 0, false, 0, []⟩
  let b : Cfg := ⟨0, 0, 1, true, 0, []⟩
  apply not_dropLastField_of ProjectPair.shape a b [some 5, some 7]
  · rfl
  · rfl
  · native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ProjectPair_DropLastField
