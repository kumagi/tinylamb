import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-!
Window execution must preserve peer membership and frame results at ordering boundaries.

Refuted by counterexample

- `ProportionalTile` — The commonly proposed scaled-row formula describes every NTILE
  bucket.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_ProportionalTile

open TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

theorem refute : ¬ ProportionalTile.Goal := by
  intro h
  have hh := h 6 4 3 (by decide) (by decide)
  have hbad : ¬ tile 6 4 3 = 1 + 3*4/6 := by native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_ProportionalTile
