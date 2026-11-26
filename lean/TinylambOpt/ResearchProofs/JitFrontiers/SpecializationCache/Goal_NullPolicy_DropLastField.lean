import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Refuted by counterexample

- `NullPolicy.DropLastField` — Speculative cache compaction: is removing the final key
  component observationally safe?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_DropLastField

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.NullPolicy
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem refute : ¬ NullPolicy.DropLastField.Goal := by
  let a : Cfg := ⟨1, 0, 0, true, 0, []⟩
  let b : Cfg := {a with literal := 2}
  apply not_dropLastField_of NullPolicy.shape a b [none]
  · rfl
  · rfl
  · native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_DropLastField
