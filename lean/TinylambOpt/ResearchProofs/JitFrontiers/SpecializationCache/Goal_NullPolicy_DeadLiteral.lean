import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `NullPolicy.DeadLiteral` — `∀ c k, c.mode=false → shape.key {c with
  literal:=k}=shape.key c`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_DeadLiteral

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.NullPolicy
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : NullPolicy.DeadLiteral.Goal := by
  intro c k h
  unfold NullPolicy.shape
  dsimp
  simp [h]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_DeadLiteral
