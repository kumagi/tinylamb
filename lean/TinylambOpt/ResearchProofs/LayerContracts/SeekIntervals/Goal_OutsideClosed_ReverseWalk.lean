import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Proved

- `OutsideClosed.ReverseWalk` — Descending index iteration can reverse both the seek
  order and each local scan.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_OutsideClosed_ReverseWalk

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

theorem sound : OutsideClosed.ReverseWalk.Goal := by
  exact Common.reverseWalk_of_exact OutsideClosed.model Cores.outsideClosed_exact Cores.outsideClosed_sep

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_OutsideClosed_ReverseWalk
