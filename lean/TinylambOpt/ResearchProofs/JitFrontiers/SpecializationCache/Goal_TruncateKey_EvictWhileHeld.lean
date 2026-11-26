import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_TruncateKey_EvictWhileHeld

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.TruncateKey
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem hone : oneVersion TruncateKey.shape := by
  intro c r hacc
  unfold TruncateKey.shape
  dsimp
  cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot <;> simp [mapV, Int.add_emod]
private theorem hsu : substitute TruncateKey.shape := by
  apply substitute_of TruncateKey.shape
  · exact hone
  ·
    intro a b hk r
    cases a with | mk al asc as am ae atab =>
    cases b with | mk bl bsc bs bm be btab =>
    unfold TruncateKey.shape at hk ⊢
    dsimp at hk ⊢
    simp at hk
    rcases hk with ⟨hs, hl⟩
    have hs' : as = bs := by omega
    subst bs
    rw [hl]
private theorem hcall : ∀ pool c r, call TruncateKey.shape pool c r = TruncateKey.shape.reference c r := by
  exact call_eq_reference TruncateKey.shape hone hsu

theorem sound : TruncateKey.EvictWhileHeld.Goal := by
  exact evictWhileHeld_of TruncateKey.shape hcall hsu

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_TruncateKey_EvictWhileHeld
