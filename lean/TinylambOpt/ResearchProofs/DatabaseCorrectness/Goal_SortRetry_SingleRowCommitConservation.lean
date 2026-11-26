import TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

/-!
A successful `ready` consumes exactly one row: the emitted row is the cursor's
head, the returned cursor holds the tail, and `emitted` increases by one. This
is the conservation law that makes a row-level commit observable once.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_SingleRowCommitConservation

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

theorem sound : SingleRowCommitConservation.Goal := by
  intro s r after h
  rcases s with ⟨rows, emitted⟩
  cases rows with
  | nil => simp [next] at h
  | cons r' rs =>
      simp [next] at h
      rcases h with ⟨hr, ha⟩
      subst r
      subst after
      simp

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_SingleRowCommitConservation
