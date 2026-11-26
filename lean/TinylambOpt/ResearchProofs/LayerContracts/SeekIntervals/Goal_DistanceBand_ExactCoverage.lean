import TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common_SeekIntervals
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
import TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

/-!
An index range plan must cover every qualifying key and preserve any gaps excluded by
the SQL predicate.

Proved

- `DistanceBand.ExactCoverage` — Sum of span widths gives the exact qualifying domain
  population.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_DistanceBand_ExactCoverage

open TinylambOpt.ResearchGoals.LayerContracts.SeekIntervals
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Common
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Cores
open TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Specifics

theorem sound : DistanceBand.ExactCoverage.Goal := by
  exact Common.coverage_of_exact DistanceBand.model Cores.distanceBand_exact Cores.distanceBand_sep

end TinylambOpt.ResearchProofs.LayerContracts.SeekIntervals.Goal_DistanceBand_ExactCoverage
