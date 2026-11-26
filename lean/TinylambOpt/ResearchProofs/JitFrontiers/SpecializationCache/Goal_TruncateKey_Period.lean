import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `TruncateKey.Period` — `∀ c n, shape.key {c with
  literal:=c.literal+65536*n}=shape.key c`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_TruncateKey_Period

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.TruncateKey
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : TruncateKey.Period.Goal := by
  intro c n
  unfold TruncateKey.shape
  dsimp
  rw [Int.add_emod]
  simp

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_TruncateKey_Period
