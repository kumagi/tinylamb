import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `ConstantLane.OneVersion` — A fresh specialization is correct under its caller-side
  entry check.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ConstantLane_OneVersion

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.ConstantLane
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : ConstantLane.OneVersion.Goal := by
  intro c r hacc
  unfold ConstantLane.shape at hacc ⊢
  dsimp at hacc ⊢
  cases h : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
  | null =>
      rw [h] at hacc
      have hf : (Out.null == Out.val c.literal) = false := by rfl
      rw [hf] at hacc
      cases hacc
  | fault =>
      rw [h] at hacc
      have hf : (Out.fault == Out.val c.literal) = false := by rfl
      rw [hf] at hacc
      cases hacc
  | val n =>
      rw [h] at hacc
      have hb : (Out.val n == Out.val c.literal) = (n == c.literal) := by rfl
      rw [hb] at hacc
      have hnl : n = c.literal := beq_iff_eq.mp hacc
      rw [hnl]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ConstantLane_OneVersion
