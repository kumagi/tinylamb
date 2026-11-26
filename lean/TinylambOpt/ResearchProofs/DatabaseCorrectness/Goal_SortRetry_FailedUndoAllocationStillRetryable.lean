import TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

/-!
Failure to allocate the undo buffer already drops the head row from the
returned cursor. A later `ready` therefore emits the *second* row, not the
first one, so the strong "undo failure is retryable" hypothesis is false.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_FailedUndoAllocationStillRetryable

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

theorem refute : ¬ FailedUndoAllocationStillRetryable.Goal := by
  intro h
  have got := h ⟨[(1, 5), (1, 6)], 0⟩
  simp [next] at got

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_FailedUndoAllocationStillRetryable
