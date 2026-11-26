import TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
import TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common_SpecializationCache

/-!
Reusing compiled SQL expressions depends on which specialization inputs preserve the
same observable behavior.

Proved

- `ConstantLane.ColdMiss` — `∀ c r, get r c.slot≠.val c.literal → call shape [] c r=get
  r c.slot`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ConstantLane_ColdMiss

open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache
open TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.ConstantLane
open TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Common
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites

theorem sound : ConstantLane.ColdMiss.Goal := by
  intro c r h
  unfold call
  simp [lookup]
  cases hg : TinylambOpt.ResearchGoals.JitFrontiers.SpecializationCache.get r c.slot with
  | null =>
      have heq : (Out.null == Out.val c.literal) = false := by rfl
      simp [ConstantLane.shape, hg, heq]
  | fault =>
      have heq : (Out.fault == Out.val c.literal) = false := by rfl
      simp [ConstantLane.shape, hg, heq]
  | val n =>
      have heq : (Out.val n == Out.val c.literal) = (n == c.literal) := by rfl
      have hne : n ≠ c.literal := by
        intro hnl; apply h; rw [hg, hnl]
      simp [ConstantLane.shape, hg, heq, hne]

end TinylambOpt.ResearchProofs.JitFrontiers.SpecializationCache.Goal_ConstantLane_ColdMiss
