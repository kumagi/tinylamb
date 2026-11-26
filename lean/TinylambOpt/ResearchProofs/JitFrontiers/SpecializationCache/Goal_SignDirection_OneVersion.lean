import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `SignDirection.OneVersion` — A fresh specialization is correct under its caller-side
  entry check.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_SignDirection_OneVersion

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.SignDirection
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : SignDirection.OneVersion.Goal := by
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

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_SignDirection_OneVersion
