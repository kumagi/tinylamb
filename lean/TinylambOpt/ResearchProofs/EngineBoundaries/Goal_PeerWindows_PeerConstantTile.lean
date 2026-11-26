import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

/-!
Window execution must preserve peer membership and frame results at ordering boundaries.

Refuted by counterexample

- `PeerConstantTile` — Tile boundaries can cut a peer group even though rank-based
  windows agree on the peer.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_PeerConstantTile

open TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

theorem refute : ¬ PeerConstantTile.Goal := by
  intro h
  have hh := h [none,none] 0 1 none 2 (by decide) (by decide) (by decide)
  have hbad : ¬ tile 2 2 0 = tile 2 2 1 := by native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_PeerWindows_PeerConstantTile
