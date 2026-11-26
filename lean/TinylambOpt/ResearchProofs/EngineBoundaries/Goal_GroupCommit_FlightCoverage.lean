import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_FlightCoverage

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

theorem sound : FlightCoverage.Goal := by
  intro s endLSN ws h
  simp [ready, step, h]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_FlightCoverage
