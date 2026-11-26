import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Proved

- `TwoLiterals.AdjacentMerge` — `∀ p, p.b = p.a+1 → p.b < p.cap → runs ((List.range
  p.cap).map (model.test p)) = 1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_TwoLiterals_AdjacentMerge

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

theorem sound : TwoLiterals.AdjacentMerge.Goal := by
  exact Specifics.twoLiterals_adjacentMerge

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_TwoLiterals_AdjacentMerge
