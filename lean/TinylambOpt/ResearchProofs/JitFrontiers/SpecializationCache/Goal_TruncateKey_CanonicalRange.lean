import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `TruncateKey.CanonicalRange` — `∀ c r v, shape.code c r=.val v → 0≤v ∧ v<65536`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_TruncateKey_CanonicalRange

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.TruncateKey
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : TruncateKey.CanonicalRange.Goal := by
  intro c r v hv
  unfold TruncateKey.shape at hv
  dsimp at hv
  cases hget : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot
  · simp [hget, mapV] at hv
  · simp [hget, mapV] at hv
  · simp [hget, mapV] at hv
    subst v
    rw [Int.add_emod]
    constructor
    · apply Int.emod_nonneg
      native_decide
    · apply Int.emod_lt_of_pos
      native_decide

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_TruncateKey_CanonicalRange
