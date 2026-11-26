import TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

/-! Different snapshots never share statistics entries.

The live `(owner, epoch)` may be equal while the snapshot differs, so the
thread cache must be discarded for that reader. SQL execution may keep the
live catalog generation and all of its other cached entries, but a reader
pinned to an older snapshot must fetch its own statistics from the oracle. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_SameLiveEpochDifferentSnapshot

open TinylambOpt.ResearchGoals.DatabaseCorrectness.StatisticsCache

theorem sound : SameLiveEpochDifferentSnapshot.Goal := by
  intro oracle c view key ownerEq epochEq snapshotNe
  have hs : (c.view == view) = false := by
    cases hb : c.view == view
    · rfl
    · have hview : c.view = view := beq_iff_eq.mp hb
      exfalso
      apply snapshotNe
      rw [← hview]
  have hsprop : ¬ ((c.view == view) = true) := by simp [hs]
  dsimp [lookup]
  rw [ite_eq_right hsprop]
  simp only [List.lookup_nil]
  cases hor : oracle view key <;> simp [hor]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.StatisticsCache_SameLiveEpochDifferentSnapshot
