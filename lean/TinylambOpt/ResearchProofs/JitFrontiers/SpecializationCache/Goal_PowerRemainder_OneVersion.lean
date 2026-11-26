import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `PowerRemainder.OneVersion` — A fresh specialization is correct under its caller-side
  entry check.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_PowerRemainder_OneVersion

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.PowerRemainder
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : PowerRemainder.OneVersion.Goal := by
  intro c r hacc
  unfold PowerRemainder.shape at hacc ⊢
  dsimp at hacc ⊢
  cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot <;> simp [mapV]
  rename_i x
  rw [h] at hacc
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hacc
  have hx0 : 0 ≤ x := hacc.2
  have hmax : max x 0 = x := by omega
  rw [hmax]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_PowerRemainder_OneVersion
