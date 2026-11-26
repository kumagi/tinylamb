import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

/-!
A commit batching policy must preserve the durability and visibility guarantees of
committed SQL transactions.

Proved

- `FlightCoverage` — Attaching waiters to an in-flight sync cannot certify more than
  its captured end.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_FlightCoverage

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

theorem sound : FlightCoverage.Goal := by
  intro s endLSN ws h
  simp [ready, step, h]

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_FlightCoverage
