import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `PowerRemainder.ReplayBatch` — Reuse in a whole batch preserves the first failing
  ordinal as well as the value sequence.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_PowerRemainder_ReplayBatch

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.PowerRemainder
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem hone : oneVersion PowerRemainder.shape := by
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
private theorem hsu : substitute PowerRemainder.shape := by
  apply substitute_of PowerRemainder.shape
  · exact hone
  ·
    intro a b hk r
    cases a with | mk al asc as am ae atab =>
    cases b with | mk bl bsc bs bm be btab =>
    unfold PowerRemainder.shape at hk ⊢
    dsimp at hk ⊢
    simp at hk
    rcases hk with ⟨hs, hsc⟩
    have hs' : as = bs := by omega
    subst bs
    subst bsc
    rfl
private theorem hcall : ∀ pool c r, call PowerRemainder.shape pool c r = PowerRemainder.shape.reference c r := by
  exact call_eq_reference PowerRemainder.shape hone hsu

theorem sound : PowerRemainder.ReplayBatch.Goal := by
  exact replayBatch_of PowerRemainder.shape hcall

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_PowerRemainder_ReplayBatch
