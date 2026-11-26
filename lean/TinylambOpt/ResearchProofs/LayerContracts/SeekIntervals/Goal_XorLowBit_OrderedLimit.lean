import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Proved

- `XorLowBit.OrderedLimit` — With sorted index entries, early LIMIT after seeking
  preserves the ordered prefix.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_XorLowBit_OrderedLimit

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

theorem sound : XorLowBit.OrderedLimit.Goal := by
  exact Common.orderedLimit_of_exact XorLowBit.model Cores.xorLowBit_exact Cores.xorLowBit_sep

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_XorLowBit_OrderedLimit
