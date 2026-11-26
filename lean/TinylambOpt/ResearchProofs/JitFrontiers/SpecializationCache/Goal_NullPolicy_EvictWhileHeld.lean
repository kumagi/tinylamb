import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `NullPolicy.EvictWhileHeld` — A retained artifact and a fresh lookup after eviction
  agree on a guarded call.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_EvictWhileHeld

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.NullPolicy
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem hone : oneVersion NullPolicy.shape := by
  intro c r hacc
  unfold NullPolicy.shape
  dsimp
  cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
  | null =>
      have hnn : (Out.null == Out.null) = true := by rfl
      by_cases hm : c.mode = true <;> simp [h, hm, hnn]
  | fault =>
      have hfn : (Out.fault == Out.null) = false := by rfl
      by_cases hm : c.mode = true <;> simp [h, hm, hfn]
  | val n =>
      have hvn : (Out.val n == Out.null) = false := by rfl
      by_cases hm : c.mode = true <;> simp [h, hm, hvn]
private theorem hsu : substitute NullPolicy.shape := by
  apply substitute_of NullPolicy.shape
  · exact hone
  ·
    intro a b hk r
    by_cases ham : a.mode = true
    · by_cases hbm : b.mode = true
      · simp [NullPolicy.shape, ham, hbm] at hk ⊢
        rcases hk with ⟨hs, hl⟩
        have hsl : a.slot = b.slot := by omega
        rw [hsl, hl]
      · simp [NullPolicy.shape, ham, hbm] at hk
    · by_cases hbm : b.mode = true
      · simp [NullPolicy.shape, ham, hbm] at hk
      · simp [NullPolicy.shape, ham, hbm] at hk ⊢
        have hsl : a.slot = b.slot := by omega
        rw [hsl]
private theorem hcall : ∀ pool c r, call NullPolicy.shape pool c r = NullPolicy.shape.reference c r := by
  exact call_eq_reference NullPolicy.shape hone hsu

theorem sound : NullPolicy.EvictWhileHeld.Goal := by
  exact evictWhileHeld_of NullPolicy.shape hcall hsu

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_NullPolicy_EvictWhileHeld
