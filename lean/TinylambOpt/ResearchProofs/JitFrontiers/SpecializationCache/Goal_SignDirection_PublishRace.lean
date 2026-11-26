import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `SignDirection.PublishRace` — Two published equivalent entries can be discovered in
  either order without changing the result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_SignDirection_PublishRace

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.SignDirection
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem hone : oneVersion SignDirection.shape := by
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
private theorem hsu : substitute SignDirection.shape := by
  apply substitute_of SignDirection.shape
  · exact hone
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
private theorem hcall : ∀ pool c r, call SignDirection.shape pool c r = SignDirection.shape.reference c r := by
  exact call_eq_reference SignDirection.shape hone hsu

theorem sound : SignDirection.PublishRace.Goal := by
  exact publishRace_of SignDirection.shape hcall

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_SignDirection_PublishRace
