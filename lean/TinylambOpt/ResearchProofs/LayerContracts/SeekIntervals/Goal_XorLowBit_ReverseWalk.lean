import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Proved

- `XorLowBit.ReverseWalk` — Descending index iteration can reverse both the seek order
  and each local scan.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_XorLowBit_ReverseWalk

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

theorem sound : XorLowBit.ReverseWalk.Goal := by
  exact Common.reverseWalk_of_exact XorLowBit.model Cores.xorLowBit_exact Cores.xorLowBit_sep

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_XorLowBit_ReverseWalk
