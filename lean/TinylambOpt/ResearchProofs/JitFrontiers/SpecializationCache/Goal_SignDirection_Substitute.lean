import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_SignDirection_Substitute

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.SignDirection
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : SignDirection.Substitute.Goal := by
  apply substitute_of SignDirection.shape
  ·
    intro c r hacc
    unfold SignDirection.shape
    dsimp
    cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot <;> simp [mapV]
    rename_i x
    by_cases hx : x = c.literal
    · subst x
      simp
    · by_cases hm : c.mode
      · by_cases hlt : x < c.literal
        · simp [hm, hlt]
          omega
        · have hgt : c.literal < x := by omega
          have hltb : decide (x < c.literal) = false := by simp [hlt]
          simp [hm, hgt, hltb, hx]
      · by_cases hlt : x < c.literal
        · have hltb : decide (x < c.literal) = true := by simp [hlt]
          simp [hm, hltb, hx]
        · have hltb : decide (x < c.literal) = false := by simp [hlt]
          simp [hm, hltb]
  ·
    intro a b hk r
    cases a with | mk al asc as am ae atab =>
    cases b with | mk bl bsc bs bm be btab =>
    unfold SignDirection.shape at hk ⊢
    dsimp at hk ⊢
    simp at hk
    rcases hk with ⟨hs, hl, hm⟩
    have hs' : as = bs := by omega
    have hm' : am = bm := by
      cases am <;> cases bm <;> simp at hm ⊢
    subst bs
    subst bl
    subst bm
    rfl

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_SignDirection_Substitute
