import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `ProjectPair.Substitute` — A cached function compiled from another descriptor is safe
  on the new guard domain.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ProjectPair_Substitute

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.ProjectPair
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : ProjectPair.Substitute.Goal := by
  apply substitute_of ProjectPair.shape
  ·
    intro c r hacc
    unfold ProjectPair.shape
    dsimp
    by_cases hm : c.mode <;> simp [hm]
  ·
    intro a b hk r
    unfold ProjectPair.shape at hk ⊢
    dsimp at hk ⊢
    simp at hk
    have hnat : a.slot + a.mode.toNat = b.slot + b.mode.toNat := by exact_mod_cast hk
    rw [hnat]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ProjectPair_Substitute
