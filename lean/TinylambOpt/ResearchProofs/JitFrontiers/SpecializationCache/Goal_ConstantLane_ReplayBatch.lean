import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ConstantLane_ReplayBatch

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.ConstantLane
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

private theorem hone : oneVersion ConstantLane.shape := by
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
private theorem hsu : substitute ConstantLane.shape := by
  apply substitute_of ConstantLane.shape
  · exact hone
  ·
    intro a b hk r
    unfold ConstantLane.shape at hk ⊢
    dsimp at hk ⊢
    simp at hk
    rw [hk]
private theorem hcall : ∀ pool c r, call ConstantLane.shape pool c r = ConstantLane.shape.reference c r := by
  exact call_eq_reference ConstantLane.shape hone hsu

theorem sound : ConstantLane.ReplayBatch.Goal := by
  exact replayBatch_of ConstantLane.shape hcall

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ConstantLane_ReplayBatch
