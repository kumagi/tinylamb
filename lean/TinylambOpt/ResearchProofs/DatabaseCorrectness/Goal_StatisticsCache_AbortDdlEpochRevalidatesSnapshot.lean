import TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

/-! Aborted DDL revalidates statistics through an epoch bump.

The new epoch differs from the cached one, so even a reader pinned to the old
snapshot cannot reuse its entries. SQL execution may abandon a DDL attempt and
still let the next statistics lookup observe the catalog at the new epoch,
without requiring the snapshot number to change. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_AbortDdlEpochRevalidatesSnapshot

open TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

theorem sound : AbortDdlEpochRevalidatesSnapshot.Goal := by
  intro c oracle k
  dsimp [lookup]
  have hs : (c.view == { c.view with epoch := c.view.epoch + 1 }) = false := by
    cases hb : c.view == { c.view with epoch := c.view.epoch + 1 }
    · rfl
    · have hview : c.view = { c.view with epoch := c.view.epoch + 1 } := beq_iff_eq.mp hb
      have : c.view.epoch = c.view.epoch + 1 := by simpa using congrArg View.epoch hview
      omega
  have hsprop : ¬ (((c.view == { c.view with epoch := c.view.epoch + 1 }) = true)) := by simp [hs]
  rw [ite_eq_right hsprop]
  simp only [List.lookup_nil]
  cases hork : oracle { c.view with epoch := c.view.epoch + 1 } k <;> simp [hork]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_AbortDdlEpochRevalidatesSnapshot
