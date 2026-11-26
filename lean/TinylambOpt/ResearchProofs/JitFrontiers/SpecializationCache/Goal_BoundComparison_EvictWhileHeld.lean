import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_BoundComparison_EvictWhileHeld

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.BoundComparison
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem hone : oneVersion BoundComparison.shape := by
  intro c r hacc
  unfold BoundComparison.shape
  dsimp
  cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot <;> simp [mapV]
  rename_i x
  by_cases hlt : x < c.literal <;> simp [hlt]
private theorem hsu : substitute BoundComparison.shape := by
  apply substitute_of BoundComparison.shape
  · exact hone
  ·
    intro a b hk r
    cases a with | mk al asc as am ae atab =>
    cases b with | mk bl bsc bs bm be btab =>
    unfold BoundComparison.shape at hk ⊢
    dsimp at hk ⊢
    simp at hk
    rcases hk with ⟨hs, hl⟩
    have hs' : as = bs := by omega
    subst bs
    subst bl
    rfl
private theorem hcall : ∀ pool c r, call BoundComparison.shape pool c r = BoundComparison.shape.reference c r := by
  exact call_eq_reference BoundComparison.shape hone hsu

theorem sound : BoundComparison.EvictWhileHeld.Goal := by
  exact evictWhileHeld_of BoundComparison.shape hcall hsu

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_BoundComparison_EvictWhileHeld
