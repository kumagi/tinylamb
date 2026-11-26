import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Proved

- `RingSlice.SameEdge` — `∀ p, p.a = p.b → ranges model p = []`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_RingSlice_SameEdge

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

theorem sound : RingSlice.SameEdge.Goal := by
  exact Specifics.ringSlice_sameEdge

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_RingSlice_SameEdge
