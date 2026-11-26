import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `TruncateKey.OneVersion` — A fresh specialization is correct under its caller-side
  entry check.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_TruncateKey_OneVersion

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.TruncateKey
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : TruncateKey.OneVersion.Goal := by
  intro c r hacc
  unfold TruncateKey.shape
  dsimp
  cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot <;> simp [mapV, Int.add_emod]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_TruncateKey_OneVersion
