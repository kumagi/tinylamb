import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.SeekMinimumModels

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Proved

- `XorLowBit.SeekMinimum` — The emitted seek count equals the number of maximal
  contiguous true regions.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_XorLowBit_SeekMinimum

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.SeekMinimumModels

theorem sound : XorLowBit.SeekMinimum.Goal := by
  exact xorLowBit_seekMin

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_XorLowBit_SeekMinimum
