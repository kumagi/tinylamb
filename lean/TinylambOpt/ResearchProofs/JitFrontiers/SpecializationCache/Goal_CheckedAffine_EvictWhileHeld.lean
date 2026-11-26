import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_CheckedAffine_EvictWhileHeld

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.CheckedAffine
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem hone : oneVersion CheckedAffine.shape := by
  intro c r hacc
  unfold CheckedAffine.shape at hacc ⊢
  dsimp at hacc ⊢
  cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot <;> simp [mapV]
  rename_i x
  rw [h] at hacc
  simp only [fits, Bool.and_eq_true, decide_eq_true_eq] at hacc
  have hmul : fits 64 (x * c.scale) := by
    unfold fits
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    omega
  have hadd : fits 64 (x * c.scale + c.literal) := by
    unfold fits
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    omega
  have hw1 := word_of_fits (x * c.scale) hmul
  have hw2 := word_of_fits (x * c.scale + c.literal) hadd
  simp [hw1, hw2, checked, hmul, hadd]
private theorem hsu : substitute CheckedAffine.shape := by
  apply substitute_of CheckedAffine.shape
  · exact hone
  ·
    intro a b hk r
    cases a with | mk al asc as am ae atab =>
    cases b with | mk bl bsc bs bm be btab =>
    unfold CheckedAffine.shape at hk ⊢
    dsimp at hk ⊢
    simp at hk
    rcases hk with ⟨hs, hsc, hl⟩
    have hs' : as = bs := by omega
    subst bs
    subst bsc
    subst bl
    rfl
private theorem hcall : ∀ pool c r, call CheckedAffine.shape pool c r = CheckedAffine.shape.reference c r := by
  exact call_eq_reference CheckedAffine.shape hone hsu

theorem sound : CheckedAffine.EvictWhileHeld.Goal := by
  exact evictWhileHeld_of CheckedAffine.shape hcall hsu

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_CheckedAffine_EvictWhileHeld
