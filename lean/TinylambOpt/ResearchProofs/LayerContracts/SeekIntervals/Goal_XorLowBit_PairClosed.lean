import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Proved

- `XorLowBit.PairClosed` — `∀ p x, p.a%2 = 0 → p.b%2 = 1 → model.test p x = (p.a ≤ x &&
  x ≤ p.b)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_XorLowBit_PairClosed

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

theorem sound : XorLowBit.PairClosed.Goal := by
  exact Specifics.xorLowBit_pairClosed

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_XorLowBit_PairClosed
