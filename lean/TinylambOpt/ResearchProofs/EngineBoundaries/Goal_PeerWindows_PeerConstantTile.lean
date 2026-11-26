import TinylambOpt.ResearchGoals.EngineBoundaries.PeerWindows

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
