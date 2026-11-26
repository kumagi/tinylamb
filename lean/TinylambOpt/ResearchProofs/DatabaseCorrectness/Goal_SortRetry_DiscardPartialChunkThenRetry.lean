import TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

/-!
Restarting a chunk builder from a cursor that already emitted `k` rows cannot
simply drop those `k` rows from the source: the dropped prefix is no longer
available to fill the first slots. A concrete three-row source shows the
resulting chunk differs from the original.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_DiscardPartialChunkThenRetry

open TinylambOpt.ResearchGoals.DatabaseCorrectness.SortRetry

theorem refute : ¬ DiscardPartialChunkThenRetry.Goal := by
  intro h
  have got := h [(1, 1), (2, 2), (3, 3)] 1 2 (by omega) (by decide)
  simp at got

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_SortRetry_DiscardPartialChunkThenRetry
