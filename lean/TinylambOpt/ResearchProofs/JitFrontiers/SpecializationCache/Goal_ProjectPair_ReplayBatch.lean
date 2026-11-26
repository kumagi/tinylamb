import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `ProjectPair.ReplayBatch` — Reuse in a whole batch preserves the first failing
  ordinal as well as the value sequence.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ProjectPair_ReplayBatch

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.ProjectPair
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem hone : oneVersion ProjectPair.shape := by
  intro c r hacc
  unfold ProjectPair.shape
  dsimp
  by_cases hm : c.mode <;> simp [hm]
private theorem hsu : substitute ProjectPair.shape := by
  apply substitute_of ProjectPair.shape
  · exact hone
  ·
    intro a b hk r
    unfold ProjectPair.shape at hk ⊢
    dsimp at hk ⊢
    simp at hk
    have hnat : a.slot + a.mode.toNat = b.slot + b.mode.toNat := by exact_mod_cast hk
    rw [hnat]
private theorem hcall : ∀ pool c r, call ProjectPair.shape pool c r = ProjectPair.shape.reference c r := by
  exact call_eq_reference ProjectPair.shape hone hsu

theorem sound : ProjectPair.ReplayBatch.Goal := by
  exact replayBatch_of ProjectPair.shape hcall

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ProjectPair_ReplayBatch
